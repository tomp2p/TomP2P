import net.tomp2p.p2p.TaskRPC;


public class Misc {
	if (isEnableTaskRPC()) { // create task manager, which is needed by the task RPC 
        TaskRPC taskRPC = new
    TaskRPC(peerBean, connectionBean); peer.setTaskRPC(taskRPC); } if (isEnablePeerExchangeRPC()) { 
        //replication for trackers, which needs PEX //TODO: enable again //
        TrackerStorageReplication
    trackerStorageReplication = new TrackerStorageReplication(peer, // peer.getPeerExchangeRPC(),
    peer.getPendingFutures(), storageTracker, // configuration.isForceTrackerTCP());
    //replicationTracker.addResponsibilityListener(trackerStorageReplication); } 
}
