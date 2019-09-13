defmodule Explorer.BlockRefetcher do
  @moduledoc """
  A package of data that contains zero or more transactions, the hash of the previous block ("parent"), and optionally
  other data. Because each block (except for the initial "genesis block") points to the previous block, the data
  structure that they form is called a "blockchain".
  """

  use Explorer.Schema

  alias Explorer.Chain.Block

  @optional_attrs ~w(first_block_number last_block_number)a

  @required_attrs ~w(name)a

  @typedoc """
   * `name` - the name identifing the refetcher
   * `first_block_number` - the number since which the fetcher has to start checking (excusive)
   * `first_block_number` - the number at which the fetcher has to stop checking (inclusive)
  """
  @type t :: %__MODULE__{
          name: String.t(),
          first_block_number: Block.block_number() | nil,
          last_block_number: Block.block_number() | nil,
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @primary_key {:name, :string, autogenerate: false}
  schema "block_refetchers" do
    field(:first_block_number, :integer)
    field(:last_block_number, :integer)

    timestamps()
  end

  def changeset(%__MODULE__{} = block_refetcher, attrs) do
    block_refetcher
    |> cast(attrs, @required_attrs ++ @optional_attrs)
    |> validate_required(@required_attrs)
    |> unique_constraint(:hash, name: :blocks_pkey)
  end

  def make_from_env(name) when is_atom(name) do
    string_name = Atom.to_string(name)
    env_vals = Application.get_env(:explorer, name, [])

    first = env_vals[:first_block_number]
    last = env_vals[:last_block_number]

    changeset(%__MODULE__{}, %{name: string_name, first_block_number: first, last_block_number: last})
  end

  @doc """
  Utility function to get a fetcher by its name (atom or string)
  """
  def fetch(name) when is_atom(name), do: fetch(Atom.to_string(name))

  def fetch(name) when is_binary(name) do
    from(
      block_refetcher in __MODULE__,
      where: block_refetcher.name == ^name
    )
  end

  @doc """
  Fetches the status from the repo and if there is none it gets the values from
  the environment variables and stores them as well.
  """
  def get_starting_numbers(repo, fetcher_name) do
    fetcher_name
    |> fetch()
    |> repo.one()
    |> case do
      nil ->
        fetcher_name
        |> make_from_env()
        |> repo.insert!()

      value ->
        value
    end
  end

  @doc """
  Updates the status in the repo with the new last_block_number
  """
  def update_refetcher_last_number(repo, fetcher_name, number) do
    fetcher_name
    |> fetch()
    |> repo.one!()
    |> with_last(number)
    |> repo.update!()

    {:ok, number}
  end

  @doc """
  Updates the status in the repo with the new first_block_number
  """
  def update_refetcher_first_number(repo, fetcher_name, number) do
    fetcher_name
    |> fetch()
    |> repo.one!()
    |> with_first(number)
    |> repo.update!()

    {:ok, number}
  end

  @doc """
  Returns a changeset with `:first_block_number` set as chosen
  """
  def with_first(%__MODULE__{} = block_refetcher, block_number) do
    changeset(block_refetcher, %{first_block_number: block_number})
  end

  @doc """
  Returns a changeset with `:last_block_number` set as chosen
  """
  def with_last(%__MODULE__{} = block_refetcher, block_number) do
    changeset(block_refetcher, %{last_block_number: block_number})
  end

  @doc """
  Returns true if the fetcher has completed its range of blocks to check.
  Note that if `:first_block_number` is `nil` it will start from the beginning and
  that if `:last_block_number` is `nil` it will never stop.
  """
  def no_work_left(first, last) when is_nil(first) or is_nil(last), do: false

  def no_work_left(first, last), do: first >= last
end
