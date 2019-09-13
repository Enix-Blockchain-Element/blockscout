defmodule Indexer.Temporary.DoubleTokenTransfers do
  @moduledoc """
  Fetches in range all the Block numbers with transactions that have multiple
  token transfers and with these:
  - removes consensus from the block
  - deletes logs and token transfers of its transactions
  """

  use Indexer.Fetcher

  require Logger

  import Ecto.Query

  alias Ecto.Multi
  alias Explorer.{BlockRefetcher, Repo}
  alias Explorer.Chain.{Block, Log, TokenTransfer, Transaction}
  alias Indexer.BufferedTask

  @behaviour BufferedTask

  @fetcher_name :double_token_transfers

  @defaults [
    flush_interval: :timer.seconds(3),
    max_batch_size: 1,
    max_concurrency: 1,
    task_supervisor: Indexer.Temporary.DoubleTokenTransfers.TaskSupervisor,
    metadata: [fetcher: @fetcher_name]
  ]

  @doc false
  # credo:disable-for-next-line Credo.Check.Design.DuplicatedCode
  def child_spec([init_options, gen_server_options]) do
    merged_init_opts =
      @defaults
      |> Keyword.merge(init_options)
      |> Keyword.put(:state, {})

    Supervisor.child_spec({BufferedTask, [{__MODULE__, merged_init_opts}, gen_server_options]}, id: __MODULE__)
  end

  @impl BufferedTask
  def init(initial, reducer, _) do
    %{first_block_number: first, last_block_number: last} = BlockRefetcher.get_starting_numbers(Repo, @fetcher_name)

    if BlockRefetcher.no_work_left(first, last) do
      {0, []}
    else
      starting_query =
        from(
          b in Block,
          # goes from latest to newest
          order_by: [desc: b.number],
          distinct: true,
          select: b.number
        )

      query =
        starting_query
        |> where_first_block_number(first)
        |> where_last_block_number(last)

      {:ok, final} = Repo.stream_reduce(query, initial, &reducer.(&1, &2))

      final
    end
  end

  @impl BufferedTask
  def run([number], _) do
    # Enforce ShareLocks tables order (see docs: sharelocks.md)
    multi =
      Multi.new()
      |> Multi.run(:transaction_hashes, fn repo, _ ->
        query =
          from(
            t in Transaction,
            where: t.block_number == ^number,
            select: t.hash
          )

        {:ok, repo.all(query)}
      end)
      |> Multi.run(:needs_reindexing, fn repo, %{transaction_hashes: hashes} ->
        if Enum.empty?(hashes) do
          {:ok, false}
        else
          # checks if among the transactions there is at least one with multiple
          # token_transfers
          query =
            from(
              tt in TokenTransfer,
              where:
                tt.transaction_hash in ^hashes and
                  fragment(
                    """
                    EXISTS (SELECT 1
                    FROM token_transfers AS other_transfer
                    WHERE other_transfer.transaction_hash = ?
                    AND other_transfer.log_index <> ?
                    )
                    """,
                    tt.transaction_hash,
                    tt.log_index
                  ),
              select: %{result: 1}
            )

          {:ok, repo.exists?(query)}
        end
      end)
      |> Multi.run(:remove_blocks_consensus, fn repo, %{needs_reindexing: needs_reindexing} ->
        with {:ok, true} <- {:ok, needs_reindexing} do
          query =
            from(
              block in Block,
              where: block.number == ^number,
              # Enforce Block ShareLocks order (see docs: sharelocks.md)
              order_by: [asc: block.hash],
              lock: "FOR UPDATE"
            )

          {_num, result} =
            repo.update_all(
              from(b in Block, join: s in subquery(query), on: b.hash == s.hash),
              set: [consensus: false]
            )

          {:ok, result}
        end
      end)
      |> Multi.run(:remove_logs, fn repo, %{needs_reindexing: needs_reindexing, transaction_hashes: hashes} ->
        with {:ok, true} <- {:ok, needs_reindexing} do
          query =
            from(
              log in Log,
              where: log.transaction_hash in ^hashes,
              # Enforce Log ShareLocks order (see docs: sharelocks.md)
              order_by: [asc: log.transaction_hash, asc: log.index],
              lock: "FOR UPDATE"
            )

          {_num, result} =
            repo.delete_all(from(l in Log, join: s in subquery(query), on: l.transaction_hash == s.transaction_hash))

          {:ok, result}
        end
      end)
      |> Multi.run(:remove_token_transfers, fn repo,
                                               %{needs_reindexing: needs_reindexing, transaction_hashes: hashes} ->
        with {:ok, true} <- {:ok, needs_reindexing} do
          query =
            from(
              transfer in TokenTransfer,
              where: transfer.transaction_hash in ^hashes,
              # Enforce TokenTransfer ShareLocks order (see docs: sharelocks.md)
              order_by: [asc: transfer.transaction_hash, asc: transfer.log_index],
              lock: "FOR UPDATE"
            )

          {_num, result} =
            repo.delete_all(
              from(tt in TokenTransfer, join: s in subquery(query), on: tt.transaction_hash == s.transaction_hash)
            )

          {:ok, result}
        end
      end)
      |> Multi.run(:update_refetcher_status, fn repo, _ ->
        BlockRefetcher.update_refetcher_last_number(repo, @fetcher_name, number)
      end)

    try do
      multi
      |> Repo.transaction()
      |> case do
        {:ok, _res} ->
          :ok

        {:error, error} ->
          Logger.error(fn -> ["Error while handling double token transfers", inspect(error)] end)
          {:retry, [number]}
      end
    rescue
      postgrex_error in Postgrex.Error ->
        Logger.error(fn -> ["Error while handling double token transfers", inspect(postgrex_error)] end)
        {:retry, [number]}
    end
  end

  defp where_first_block_number(query, number) when is_nil(number), do: query

  defp where_first_block_number(query, number) do
    where(query, [b], b.number > ^number)
  end

  defp where_last_block_number(query, number) when is_nil(number), do: query

  defp where_last_block_number(query, number) do
    where(query, [b], b.number <= ^number)
  end
end
