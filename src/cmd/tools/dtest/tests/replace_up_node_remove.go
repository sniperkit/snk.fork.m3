/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

package dtests

import (
	"github.com/spf13/cobra"

	"github.com/sniperkit/snk.fork.m3/src/cmd/tools/dtest/harness"
	"github.com/sniperkit/snk.fork.m3cluster/shard"
	xclock "github.com/sniperkit/snk.fork.m3x/clock"
)

var (
	replaceUpNodeRemoveTestCmd = &cobra.Command{
		Use:   "replace_up_node_remove",
		Short: "Run a dtest where a node that is UP is replaced from the cluster. The replacing Node is removed as it begins bootstrapping.",
		Long: `
		Perform the following operations on the provided set of nodes:
		(1) Create a new cluster placement using all but one of the provided nodes.
		(2) Seed the nodes used in (1), with initial data on their respective file-systems.
		(3) Start the nodes from (1), and wait until they are bootstrapped.
		(4) Replace any node in the cluster with the unused node in the cluster placement.
		(5) Start the joining node's process.
		(6) Wait until any shard on the joining node is marked as available.
		(7) Remove the joining node from the cluster placement.
`,
		Example: `./dtest replace_up_node_remove --m3db-build path/to/m3dbnode --m3db-config path/to/m3dbnode.yaml --dtest-config path/to/dtest.yaml`,
		Run:     replaceUpNodeRemoveDTest,
	}
)

func replaceUpNodeRemoveDTest(cmd *cobra.Command, args []string) {
	if err := globalArgs.Validate(); err != nil {
		printUsage(cmd)
		return
	}

	logger := newLogger(cmd)
	dt := harness.New(globalArgs, logger)
	defer dt.Close()

	nodes := dt.Nodes()
	numNodes := len(nodes) - 1 // leaving one spare
	testCluster := dt.Cluster()

	logger.Infof("setting up cluster")
	setupNodes, err := testCluster.Setup(numNodes)
	panicIfErr(err, "unable to setup cluster")
	logger.Infof("setup cluster with %d nodes", numNodes)

	logger.Infof("seeding nodes with initial data")
	panicIfErr(dt.Seed(setupNodes), "unable to seed nodes")
	logger.Infof("seeded nodes")

	logger.Infof("starting cluster")
	panicIfErr(testCluster.Start(), "unable to start nodes")
	logger.Infof("started cluster with %d nodes", numNodes)

	logger.Infof("waiting until all instances are bootstrapped")
	panicIfErr(dt.WaitUntilAllBootstrapped(setupNodes), "unable to bootstrap all nodes")
	logger.Infof("all nodes bootstrapped successfully!")

	// pick first node from cluster
	nodeToReplace := setupNodes[0]
	logger.Infof("replacing node: %v", nodeToReplace.ID())
	replacementNodes, err := testCluster.ReplaceNode(nodeToReplace)
	panicIfErr(err, "unable to replace node")
	panicIf(len(replacementNodes) < 1, "no replacement nodes returned")
	logger.Infof("replaced node with: %+v", replacementNodes)

	logger.Infof("starting replacement nodes")
	// starting replacement nodes
	for _, n := range replacementNodes {
		panicIfErr(n.Start(), "unable to start node")
	}
	logger.Infof("started replacement nodes")

	// NB(prateek): ideally we'd like to wait until the node begins bootstrapping, but we don't
	// have a way to capture that node status. The rpc endpoint in m3dbnode only captures bootstrap
	// status at the database level, and m3kv only captures state once a shard is marked as bootstrapped.
	// So here we wait until any shard is marked as bootstrapped before continuing.

	// wait until any shard is bootstrapped (i.e. marked available on new node)
	replacementNode := replacementNodes[0]
	logger.Infof("waiting till any shards are bootstrapped on node: %v", replacementNode.ID())
	timeout := dt.BootstrapTimeout()
	anyBootstrapped := xclock.WaitUntil(func() bool { return dt.AnyInstanceShardHasState(replacementNode.ID(), shard.Available) }, timeout)
	panicIf(!anyBootstrapped, "all shards not available")

	// remove the node once it has a shard available
	logger.Infof("node has at least 1 shard available. removing replacement nodes")
	for _, n := range replacementNodes {
		panicIfErr(testCluster.RemoveNode(n), "unable to remove node")
	}
	logger.Infof("removed replacement nodes")
}
