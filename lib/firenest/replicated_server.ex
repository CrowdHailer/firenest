defmodule Firenest.ReplicatedServer do
  use GenServer

  alias Firenest.Topology

  @type state() :: term()
  @opaque replica_ref() :: {node(), Topology.id()}

  @callback init(arg :: term(), [replica_ref()]) ::
              {:ok, state}
              | {:ok, state(), timeout() | :hibernate}
              | {:stop, reason :: term()}

  @callback replica_up(replica_ref(), state()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason :: term(), new_state}
            when new_state: state()

  @callback replica_down(replica_ref(), state()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason :: term(), new_state}
            when new_state: state()

  @callback handle_call(request :: term(), GenServer.from(), state()) ::
              {:reply, reply, new_state}
              | {:reply, reply, new_state, timeout() | :hibernate}
              | {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason, reply, new_state}
              | {:stop, reason, new_state}
            when reply: term(), new_state: state(), reason: term()

  @callback handle_cast(request :: term(), state()) ::
              {:noreply, new_state}
              | {:noreply, new_state, timeout() | :hibernate}
              | {:stop, reason :: term(), new_state}
            when new_state: state()

  @callback terminate(reason, state()) :: term()
            when reason: :normal | :shutdown | {:shutdown, term()}

  def start_link(mod, arg, opts \\ []) do
    name = Keyword.fetch!(opts, :name)
    topology = Keyword.fetch!(opts, :topology)
    GenServer.start_link(__MODULE__, {mod, arg, topology, name}, opts)
  end

  def call(server, msg, timeout \\ 5_000) do
    GenServer.call(server, msg, timeout)
  end

  def cast(server, msg) do
    GenServer.cast(server, msg)
  end

  def reply(from, reply) do
    GenServer.reply(from, reply)
  end

  # TODO: do we also need a way to send from outside the process?
  def replica_send({node, _id}, msg) do
    {topology, name} = Process.get(__MODULE__)
    Topology.send(topology, node, name, msg)
  end

  @impl true
  def init({mod, arg, topology, name}) do
    Process.put(__MODULE__, {topology, name})
    state = %{name: name, topology: topology, mod: mod, int: nil}

    with {:ok, state, replicas} <- sync_named(topology, state),
         {:ok, state} <- init_mod(mod, arg, replicas, state) do
      {:ok, state}
    end
  end

  @impl true
  def handle_cast(msg, state) do
    %{mod: mod, int: int} = state

    result = safe_apply(mod, :handle_cast, [msg, int])
    handle_common(result, state)
  end

  @impl true
  def handle_call(msg, from, state) do
    %{mod: mod, int: int} = state

    case safe_apply(mod, :handle_call, [msg, from, int]) do
      {:reply, reply, int} -> {:reply, reply, %{state | int: int}}
      {:reply, reply, int, timeout} -> {:reply, reply, %{state | int: int}, timeout}
      {:stop, reason, reply, int} -> {:stop, reason, reply, %{state | int: int}}
      other -> handle_common(other, state)
    end
  end

  @impl true
  def handle_info({:named_up, node, id, name}, %{name: name} = state) do
    %{mod: mod, int: int} = state

    result = safe_apply(mod, :replica_up, [{node, id}, int])
    handle_common(result, state)
  end

  def handle_info({:named_down, node, id, name}, %{name: name} = state) do
    %{mod: mod, int: int} = state

    result = safe_apply(mod, :replica_down, [{node, id}, int])
    handle_common(result, state)
  end

  def handle_info(msg, state) do
    %{mod: mod, int: int} = state

    result = safe_apply(mod, :handle_info, [msg, int])
    handle_common(result, state)
  end

  @impl true
  def terminate(reason, state) do
    %{mod: mod, int: int} = state
    safe_apply(mod, :terminate, [reason, int])
    :ok
  end

  defp handle_common(result, state) do
    case result do
      {:noreply, int} -> {:noreply, %{state | int: int}}
      {:noreply, int, timeout} -> {:noreply, %{state | int: int}, timeout}
      {:stop, reason, int} -> {:stop, reason, %{state | int: int}}
      other -> {:stop, {:bad_return_value, other}, state}
    end
  end

  defp init_mod(mod, arg, replicas, state) do
    Process.put(:"$initial_call", {mod, :init, 2})

    try do
      mod.init(arg, replicas)
    catch
      :throw, value ->
        reason = {{:nocatch, value}, System.stacktrace()}
        {:stop, reason}
    else
      {:ok, int} -> {:ok, %{state | int: int}}
      {:ok, int, timeout} -> {:ok, %{state | int: int}, timeout}
      {:stop, reason} -> {:stop, reason}
      other -> {:stop, {:bad_return_value, other}}
    end
  end

  defp sync_named(topology, state) do
    case Topology.sync_named(topology, self()) do
      {:ok, nodes} -> {:ok, state, nodes}
      {:error, error} -> {:stop, {:sync_named, error}}
    end
  end

  @compile {:inline, safe_apply: 3}
  defp safe_apply(mod, fun, args) do
    try do
      apply(mod, fun, args)
    catch
      :throw, value ->
        :erlang.raise(:error, {:nocatch, value}, System.stacktrace())
    end
  end
end
