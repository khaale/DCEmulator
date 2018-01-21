module DCEmulator.Actors

open Akkling
open Common
open Common.Utils
open Akka.Actor
open Akka.Event


type MasterMsg = 
    | NodeConnected of string
    | NodeConfirmedTxs of string*(Tx array)
    | QueryState

type NodeMsg = 
    | Connect of IActorRef<NodeMsg> array
    | ConfirmTxs
    | ProposeTxs of Tx array

type NodeState<'a> = {
    WantPropagate: bool
    Followers: IActorRef<NodeMsg> array
    TxsToSend: Tx array
    RoundsLeft: int
    InnerState: 'a
}

type NodeBehavior<'a> = NodeState<'a> -> Tx array -> NodeState<'a>
type NodeFactory = Actor<MasterMsg> -> int -> Tx array -> IActorRef<NodeMsg>

module private Master =
    open System
    open Common.Array2DExt

    type MasterState = {
        ConfirmedTxs: Map<string,Tx array>
        ConnectedNodes: string list
    }

    // Completes connectivity matrix, to have at least one TRUE in each row and column (main diagonal excluded)
    let rec completeConnectivityMatrix m =
        let cnt = Array2D.length1 m
        let tuplify2 a b = a,b
        let incomplete = Seq.mapi tuplify2 >> Seq.filter (snd >> Seq.forall (id not)) >> Seq.map fst >> Seq.toArray
        let incompleteRowIds = incomplete <| rows m
        let incompleteColIds = incomplete <| cols m
        let isComplete = Seq.isEmpty incompleteRowIds && Seq.isEmpty incompleteColIds
        let randomFollower x = 
            match Rnd.randomInt (cnt-1) with
            | v when v >= x -> v+1
            | v -> v
        match isComplete with
        | true -> m
        | false ->
            let pairs = 
                Array.append
                    (incompleteRowIds |> Array.map (fun x -> fun () -> x,(randomFollower x)))
                    (incompleteColIds |> Array.map (fun x -> fun () -> (randomFollower x),x))
            let pair = Array.get pairs (Rnd.randomInt pairs.Length) ()
            let m' = m |> Array2D.mapi (fun i j v -> v || (i,j) = pair)                
            completeConnectivityMatrix m'
    
    // Generates an array of node and it's followers items 
    let generateFollowers mailbox cfg nodes =
        let cnt = Array.length nodes
        // Generate initial connectivity matrix, each pair of nodes connected with given probability
        let matrix = 
            Array2D.create cnt cnt false
            |> Array2D.mapi (fun i j _ -> if i = j then false else (Rnd.randomDecision cfg.ConnectivityProbability))
        logInfof mailbox "Initial connectivity matrix: %s%s" Environment.NewLine (sprintMatrix matrix)
        // In order to avoid degenerate cases: make sure that every node is folloer and followed at least once
        let completedMatrix = completeConnectivityMatrix matrix
        logInfof mailbox "Completed connectivity matrix: %s%s" Environment.NewLine (sprintMatrix completedMatrix)
        // Return node and its followers according to final connectivity matrix
        rows completedMatrix |> Seq.mapi (fun i connected ->
            let node = Array.get nodes i 
            let followers = 
                connected 
                |> Seq.mapi (fun i x -> i,x) 
                |> Seq.filter snd 
                |> Seq.map (fun x -> Array.get nodes (fst x)) 
                |> Seq.toArray
            node,followers
        )

    let masterActor system (nodeFactory:NodeFactory) cfg  = 
        spawn system "master" <| props (fun mailbox ->
            // Create nodes with initial txs
            let allTxs = [1..cfg.TxCount] |> Seq.map (fun i -> { Tx.Id = i }) |> Seq.toArray
            let nodes = 
                [1..cfg.NodesCount] 
                |> Seq.map (fun (nodeId) -> 
                    let txs = 
                        allTxs 
                        |> Seq.filter (fun _ -> Rnd.randomDecision cfg.TxCommunicationProbablity) 
                        |> Seq.toArray
                    nodeFactory mailbox nodeId txs)
                |> Seq.toArray
            logInfof mailbox "Created %d nodes, sending followers.." nodes.Length
            // Connect each node to followers
            generateFollowers mailbox cfg nodes 
            |> Seq.iter (fun (node, followers) -> node <! NodeMsg.Connect followers)
            // Process messages
            let rec loop state = 
                actor {
                    let consensusReached (s:MasterState) = 
                        match Map.count s.ConfirmedTxs with
                        | x when x = cfg.NodesCount -> true
                        | _ -> false
                    let! msg = mailbox.Receive()
                    match msg with
                    // Node successfully connected
                    | NodeConnected name -> 
                        let state' = { state with ConnectedNodes = name::state.ConnectedNodes }
                        if state'.ConnectedNodes.Length = nodes.Length 
                            then nodes |> Seq.iter (fun n -> n <! NodeMsg.ConfirmTxs)
                        return loop state'
                    // Node determined a final list of txs to confirm
                    | NodeConfirmedTxs (name,txs) -> 
                        logInfof mailbox "Node %s confirmed %d txs" name txs.Length
                        let state' = { state with MasterState.ConfirmedTxs = (Map.add name txs state.ConfirmedTxs)}
                        // Kill node actors when consensus reached
                        if consensusReached state' then 
                            logInfo mailbox "Consensus reached, killing nodes.."
                            nodes |> Seq.iter (fun n -> (retype n) <! PoisonPill.Instance)
                        return loop state'
                    // Somebody wants to know a current consensus state
                    | QueryState -> 
                        let response = 
                            match consensusReached state with
                            | true -> Some state.ConfirmedTxs
                            | false -> None
                        mailbox.Sender() <! response
                }
            loop { 
                MasterState.ConfirmedTxs = Map.ofList []; 
                MasterState.ConnectedNodes = []})

module private Node = 

    let propagateTxs (mailbox:Actor<_>) followers txs = 
        if Seq.length followers = 0 then logWarning mailbox "Nothing to propagate!"
        followers |> Seq.iter (fun node ->
            node <! ProposeTxs txs
            |> ignore)

    let nodeActor<'a> system (nodeBehavior:NodeBehavior<_>) initState  name =
        spawn system name <| props (fun mailbox ->

            logInfof mailbox "Initialized with txs: %s" <| sprintTxs initState.TxsToSend
            
            let rec loop (state:NodeState<'a>) = 
                actor {
                    let! message = mailbox.Receive()
                    match message with
                    // Received a list of followers
                    | Connect followers -> 
                        logInfof mailbox "Connected to: %s" <| sprintSeq followers (fun x -> x.Path.Name)
                        mailbox.Parent() <! MasterMsg.NodeConnected name
                        return! loop { state with NodeState.Followers = followers }
                    // Command to start confirmation process
                    | ConfirmTxs ->                        
                        propagateTxs mailbox state.Followers state.TxsToSend
                        return! loop state
                    // Received a list of txs from another node
                    | ProposeTxs otherTxs ->
                        logDebugf  mailbox "Received txs: %s" <| sprintTxs otherTxs
                        match state.RoundsLeft with
                        | 0 -> 
                            // Continue propagating confirmed set of txs in order to keep other nodes active
                            propagateTxs mailbox state.Followers state.TxsToSend
                            return! loop state
                        | _ -> 
                            let state' = nodeBehavior state otherTxs
                            logDebugf mailbox "Rounds left: %d. New txs: %s" state'.RoundsLeft <| sprintTxs state'.TxsToSend
                            // Propagate txs if needed
                            if state'.WantPropagate then propagateTxs mailbox state.Followers state'.TxsToSend
                            // Send results if we are on the last round
                            if state'.RoundsLeft = 1 then mailbox.Parent() <! MasterMsg.NodeConfirmedTxs (name, state'.TxsToSend)
                            return! loop { state' with NodeState.RoundsLeft = state'.RoundsLeft - 1 }                        
                    }
            loop initState)

/// <summary>
/// Master actor:
/// - creates actors for nodes
/// - sets up connections between nodes
/// - receives node's confirmed transactions and kills node actors when all nodes confirmed
/// - returns consensus state when asked
/// </summary>
let masterActor = Master.masterActor

/// <summary>
/// Node actor:
/// - propagates it's transactions to following nodes
/// - sends comfirmed transactions to master
/// </summary>
let nodeActor = Node.nodeActor

/// <summary>
/// Monitors dead letter queue.
/// </summary>
let deadLetterMonitorActor (system:ActorSystem) = 
    spawn system "dead-letter-monitor" <| props (actorOf (fun (msg:DeadLetter) -> 
        printfn "DeadLetter %A to %A: %A" msg.Sender msg.Recipient msg.Message |> ignored))