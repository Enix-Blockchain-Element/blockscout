defmodule Indexer.Temporary.InternalTransactionsBlockNumberTest do
  use Explorer.DataCase

  alias Explorer.BlockRefetcher
  alias Explorer.Chain.{Block}
  alias Indexer.Temporary.InternalTransactionsBlockNumber

  @fetcher_name :internal_transactions_block_number

  describe "run/2" do
    setup do
      # clear the data from the database
      @fetcher_name
      |> BlockRefetcher.fetch()
      |> Repo.one()
      |> case do
        nil -> %BlockRefetcher{name: Atom.to_string(@fetcher_name)}
        refetcher -> refetcher
      end
      |> BlockRefetcher.changeset(%{first_block_number: nil, last_block_number: nil})
      |> Repo.insert_or_update()

      :ok
    end

    test "removes consensus from blocks when one of its internal_transactions has the wrong number" do
      block = insert(:block)
      transaction = insert(:transaction) |> with_block(block)

      block_number = block.number

      insert(:internal_transaction, transaction: transaction, block_number: block_number + 1, index: 0)

      InternalTransactionsBlockNumber.run([block_number], nil)

      assert %{consensus: false} = from(b in Block, where: b.number == ^block_number) |> Repo.one()
    end
  end
end
