// Contains various implementation of node's behavior
module DCEmulator.NodeImpl

open Actors
open Common

// The simplies comliant behavior - accepts all the incoming txs and propagates them to followers.
let private mergeBehavior (state:NodeState<unit>) otherTxs = 
    let merge = Array.append state.TxsToSend >> Array.distinct >> Array.sort 
    let newTxs = merge otherTxs
    { state with 
        NodeState.TxsToSend = newTxs; 
        NodeState.WantPropagate = true }
// The simplies malicious behavior - always send nothing.
let private forgetBehavior s _ = { s with NodeState.TxsToSend = [||] }

/// <summary>
/// Comliant node behavior. Strives to consensus
/// </summary>
let compliantNode (cfg:NetworkConfig) system id txs = 
    let initState = { 
            NodeState.WantPropagate = true;
            NodeState.Followers = [||]; 
            NodeState.TxsToSend = txs 
            NodeState.RoundsLeft = cfg.RoundsCount;
            NodeState.InnerState = () }
    nodeActor system mergeBehavior initState (sprintf "node-%d-comp" id)
    
/// <summary>
/// Malicious node behavior. Strives to consensus disruption.
/// </summary>
let maliciousNode (cfg:NetworkConfig) system id txs =  
    let initState = { 
            NodeState.WantPropagate = true;
            NodeState.Followers = [||]; 
            NodeState.TxsToSend = txs; 
            NodeState.RoundsLeft = cfg.RoundsCount;
            NodeState.InnerState = () }
    nodeActor system forgetBehavior initState (sprintf "node-%d-malic" id)
