defmodule Indexer.Temporary.InternalTransactionsBlockNumber do
  @moduledoc """
  Checks that all the internal_transactions have the same block numbers as their
  transactions. Removes consensus from the for such numbers where this is not
  the case.
  """

  use Indexer.Fetcher

  require Logger

  import Ecto.Query

  alias Ecto.Multi
  alias Explorer.{BlockRefetcher, Repo}
  alias Explorer.Chain.{Block, Transaction}
  alias Indexer.BufferedTask

  @behaviour BufferedTask

  @fetcher_name :internal_transactions_block_number

  @defaults [
    flush_interval: :timer.seconds(3),
    max_batch_size: 1,
    max_concurrency: 1,
    task_supervisor: Indexer.Temporary.InternalTransactionsBlockNumber.TaskSupervisor,
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
      |> Multi.run(:needs_reindexing, fn repo, _ ->
        # checks if among the transactions with the block number there is at least one
        # with one or more internal transaction that have a different block number
        query =
          from(
            t in Transaction,
            where:
              t.block_number == ^number and
                fragment(
                  """
                  EXISTS (SELECT 1
                  FROM internal_transactions AS other_transfer
                  WHERE other_transfer.transaction_hash = ?
                  AND other_transfer.block_number <> ?
                  )
                  """,
                  t.hash,
                  t.block_number
                ),
            select: %{result: 1}
          )

        {:ok, repo.exists?(query)}
      end)
      |> Multi.run(:remove_block_consensus, fn repo, %{needs_reindexing: needs_reindexing} ->
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
          Logger.error(fn -> ["Error while handling internal_transactions with wrong block number", inspect(error)] end)
          {:retry, [number]}
      end
    rescue
      postgrex_error in Postgrex.Error ->
        Logger.error(fn ->
          ["Error while handling internal_transactions with wrong block number", inspect(postgrex_error)]
        end)

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
