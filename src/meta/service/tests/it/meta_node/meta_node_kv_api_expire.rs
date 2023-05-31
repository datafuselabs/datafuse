// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use common_base::base::tokio;
use common_base::base::tokio::time::sleep;
use common_meta_kvapi::kvapi::KVApi;
use common_meta_types::Cmd;
use common_meta_types::KVMeta;
use common_meta_types::LogEntry;
use common_meta_types::MatchSeq;
use common_meta_types::SeqV;
use common_meta_types::UpsertKV;
use common_meta_types::With;
use databend_meta::init_meta_ut;
use tracing::info;

use crate::tests::meta_node::start_meta_node_leader;
use crate::tests::meta_node::start_meta_node_non_voter;

/// Expiring kvs should be consistent on leader and followers/learners. E.g.: expiring does not depends on clock time.
///
/// - Start a leader, write kv with expiration;
/// - Assert expired kv can not be read and write.
/// - Bring up a learner, replicate logs from leader, rebuild the same state machine.
#[async_entry::test(worker_threads = 5, init = "init_meta_ut!()", tracing_span = "debug")]
async fn test_meta_node_replicate_kv_with_expire() -> anyhow::Result<()> {
    let mut log_index = 0;

    info!("--- bring up leader");
    let (_id, tc0) = start_meta_node_leader().await?;
    // initialization log, leader blank log, writing node log
    log_index += 3;

    let leader = tc0.meta_node();
    leader
        .raft
        .wait(timeout())
        .log(Some(log_index), "leader log index")
        .await?;

    let key = "expire-kv";
    let value2 = "value2";
    let now_sec = SeqV::<()>::now_ms() / 1000;

    info!("--- write a kv expiring in 3 sec");
    {
        let upsert = UpsertKV::update(key, key.as_bytes()).with(KVMeta {
            expire_at: Some(now_sec + 3),
        });

        leader.write(LogEntry::new(Cmd::UpsertKV(upsert))).await?;
        log_index += 1;
    }

    info!("--- get kv with expire now+3");
    let seq = {
        let resp = leader.get_kv(key).await?;
        let seq_v = resp.unwrap();
        assert_eq!(
            Some(KVMeta {
                expire_at: Some(now_sec + 3)
            }),
            seq_v.meta
        );
        seq_v.seq
    };

    info!("--- update kv with exact seq matching, should work before expiration");
    {
        let upsert = UpsertKV::update(key, value2.as_bytes())
            .with(MatchSeq::Exact(seq))
            .with(KVMeta {
                expire_at: Some(now_sec + 1000),
            });
        leader.write(LogEntry::new(Cmd::UpsertKV(upsert))).await?;
        log_index += 1;
    }

    info!("--- get updated kv with new expire now+1000, assert the updated value");
    {
        let resp = leader.get_kv(key).await?;
        let seq_v = resp.unwrap();
        assert_eq!(
            Some(KVMeta {
                expire_at: Some(now_sec + 1000),
            }),
            seq_v.meta
        );
        assert_eq!(value2.to_string().into_bytes(), seq_v.data);
    }

    info!("--- expire the first update: expire_at=now+3");
    sleep(Duration::from_millis(4_000)).await;

    info!("--- add new learner to receive logs, rebuild state locally");
    let (_id, tc1) = start_meta_node_non_voter(leader.clone(), 1).await?;
    // add node, change membership
    log_index += 2;

    let learner = tc1.meta_node();
    learner
        .raft
        .wait(timeout())
        .log(Some(log_index), "learner received all logs")
        .await?;

    // A learner should use the time embedded in raft-log to expire records.
    // This way on every node applying a log always get the same result.
    info!("--- get updated kv with new expire, assert the updated value");
    {
        let sm = learner.sto.state_machine.read().await;
        let resp = sm.kv_api().get_kv(key).await.unwrap();
        let seq_v = resp.unwrap();
        assert_eq!(
            Some(KVMeta {
                expire_at: Some(now_sec + 1000),
            }),
            seq_v.meta
        );
        assert_eq!(value2.to_string().into_bytes(), seq_v.data);
    }

    Ok(())
}

fn timeout() -> Option<Duration> {
    Some(Duration::from_millis(5_000))
}
