#!/bin/sh

cd build/test

cd conf_change_test && ./conf_change_test && cd ..
cd election_test && ./election_test && cd ..
cd inflights_test && ./inflights_test && cd ..
cd log_unstable_test && ./log_unstable_test && cd ..
cd memory_storage_test && ./memory_storage_test && cd ..
cd progress_test && ./progress_test && cd ..
cd raft_flow_control_test && ./raft_flow_control_test && cd ..
cd raft_log_test && ./raft_log_test && cd ..
cd rawnode_test && ./rawnode_test && cd ..
cd replication_test && ./replication_test && cd ..
cd restore_test && ./restore_test && cd ..
cd snapshot_test && ./snapshot_test && cd ..
