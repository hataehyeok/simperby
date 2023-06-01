use super::*;
use raw::RawCommit;
use simperby_network::Error;
use simperby_network::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadCommit {
    pub commit: RawCommit,
    pub hash: CommitHash,
    pub parent_hash: CommitHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchType {
    Agenda,
    AgendaProof,
    Block,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadBranch {
    pub branch_type: BranchType,
    /// The list of commit hashes in the branch, starting from
    /// **the next commit** of the `finalized` commit.
    pub commit_hashes: Vec<CommitHash>,
    /// 브랜치를 만들 때 필요한 이름을 표시하기 위해 사용.
    pub branch_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    Commit(PayloadCommit),
    Branch(PayloadBranch),
}

impl ToHash256 for Message {
    fn to_hash256(&self) -> Hash256 {
        Hash256::hash(serde_spb::to_vec(self).unwrap())
    }
}

impl DmsMessage for Message {
    fn check(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// branch를 dms로 전송하는 부분이랑 dms를 받아오는 부분을 각각 flush, update 함수에서 구현을 하고 있는 상태
/*
   flush에서는 PayloadCommit랑 PayloadBranch를 만들어서 보내는 부분이 둘 다 있어야 함.
   나중에 peer가 dms 를 받을 때 순서를 보장 받지 못함 -> 순서 매칭을 payloadBranch로, 내용을 payloadCommit으로 보내는 방식으로 구현.
*/

pub async fn flush(
    _raw: Arc<RwLock<RawRepository>>,
    _dms: Arc<RwLock<Dms<Message>>>,
) -> Result<(), Error> {
    // flush에서는 클라이언트가 _raw에서부터 브랜치를 모두 불러온 다음에
    // a-, b-의 경우를 나눠서 BranchType:: 을 지정하고 dms로 전송할 수 있도록 함.
    let last_finalized_commit_hash = _raw
        .write()
        .await
        .locate_branch(FINALIZED_BRANCH_NAME.into())
        .await?;

    let branches = _raw
        .write()
        .await
        .get_branches(last_finalized_commit_hash)
        .await?;

    for branch in branches {
        if branch.starts_with("a-") {
            let a_commit_hash = _raw.write().await.locate_branch(branch.clone()).await?;
            let commit_hashes = _raw
                .write()
                .await
                .query_commit_path(a_commit_hash, last_finalized_commit_hash)
                .await?;
            let payload_branch = PayloadBranch {
                branch_type: BranchType::Agenda,
                commit_hashes: commit_hashes.clone(),
                branch_name: branch.clone(),
            };
            let message = Message::Branch(payload_branch);
            _dms.write().await.commit_message(&message).await?;

            // 위에서 PayloadBranch를 commit_message로 보내는 것처럼 PayloadCommit에 대해 commit_message을 보내야함
            for commit_hash in commit_hashes {
                let payload_commit = PayloadCommit {
                    commit: _raw.write().await.read_commit(commit_hash).await?,
                    hash: commit_hash,
                    parent_hash: last_finalized_commit_hash,
                };

                let message = Message::Commit(payload_commit);
                _dms.write().await.commit_message(&message).await?;
                // commit_message에서 vec<PayloadCommit>을 한번에 보낼 수 없다면, commit들을 하나하나 보낸 다음에 peer가 그거 받아서 재조립해야되는데 씹재앙
                // Message Type으로 만들어서 한번에 보내는건 안되나?
            }
        } else if branch.starts_with("b-") {
            let b_commit_hash = _raw.write().await.locate_branch(branch.clone()).await?;
            let commit_hashes = _raw
                .write()
                .await
                .query_commit_path(b_commit_hash, last_finalized_commit_hash)
                .await?;
            let payload_branch = PayloadBranch {
                branch_type: BranchType::Block,
                commit_hashes: commit_hashes.clone(),
                branch_name: branch.clone(),
            };
            let message = Message::Branch(payload_branch);
            _dms.write().await.commit_message(&message).await?;

            for commit_hash in commit_hashes {
                let payload_commit = PayloadCommit {
                    commit: _raw.write().await.read_commit(commit_hash).await?,
                    hash: commit_hash,
                    parent_hash: last_finalized_commit_hash,
                };

                let message = Message::Commit(payload_commit);
                _dms.write().await.commit_message(&message).await?;
            }
        } else {
            continue;
        }
    }

    // filtering을 해서 a-~ b-~ 이 두 가지 경우를 제외하고는 아무것도 안하고 그 두 경우에는 encoding해서 send한다.

    todo!()
}

/// Updates the repository module with the latest messages from the DMS.
///
/// Note that it never finalizes a block.
/// Finalization is done by the consensus module, or the `sync` method.
/*
   예외처리 & Detail
   1. (First Commit hash of received things) == (local repo's final hash)
   2. payload commit들 순서 보장하는거 -> 근데 걍 이거 Message type을 하나 더 만드는게 나을 것 같은데
   3. PayloadBranch가 아직 도착하지 않았는데 PayloadCommit vector들이 먼저 도착 -> 순서가 올 때까지 기다려야함
       -> commit hash값으로 알 수 있을 것 같음
   4.
*/

pub async fn update(
    _raw: Arc<RwLock<RawRepository>>,
    _dms: Arc<RwLock<Dms<Message>>>,
) -> Result<(), Error> {
    let messages = _dms.write().await.read_messages().await?;
    let last_message = messages.last().unwrap().clone().message;

    // 확인해야 할 부분 :
    // semantic commit을 만들어서 raw에 업데이트 하는 과정을 for문 안에서 create_semantic_commit을 하는 식으로 처리했는데,
    // 이렇게 하는 방식이 맞는가?

    match last_message {
        Message::Branch(payloadbranch) => {
            let commit_hashes = payloadbranch.commit_hashes; // sender가 보낸 commit hash들
            let branch_name = payloadbranch.branch_name; // sender의 branch-name을 그대로 이용

            _raw.write()
                .await
                .checkout(FINALIZED_BRANCH_NAME.into())
                .await?;

            let last_finalized_commit_hash = _raw
                .write()
                .await
                .locate_branch(FINALIZED_BRANCH_NAME.into())
                .await?;

            _raw.write()
                .await
                .create_branch(branch_name.clone(), last_finalized_commit_hash)
                .await?;

            for commit in commit_hashes {
                // 모든 받아온 commit hash들에 대해서 semantic commit을 만들어서 local repo를 업데이트 시킨다.
                let semantic_commit = _raw.write().await.read_semantic_commit(commit).await?;
                _raw.write()
                    .await
                    .create_semantic_commit(semantic_commit)
                    .await?;
            }
        }
        // Commit에는 내용이랑 prev hash만 담겨있어서 여기에 해당하는 branch가 도착했는지 알 수 없음
        // Branch가 먼저 도착하면 그걸로 틀을 만들고 commit을 넣어야 될 것 같은데
        Message::Commit(Payloadcommit) => {
            let commit = Payloadcommit.commit;
            let commit_hash = Payloadcommit.hash;
            let parent_hash = Payloadcommit.parent_hash;

            //해당 커밋의 branch가 먼저 도착해는지 확인하는 과정
            //????

            let last_finalized_commit_hash = _raw
                .write()
                .await
                .locate_branch(FINALIZED_BRANCH_NAME.into())
                .await?;

            if parent_hash == last_finalized_commit_hash {
                // Receiver가 모르는 새로운 branch를 받는 과정 -> PayloadBranch가 왔는지 확인해야함
                let semantic_commit = _raw.write().await.read_semantic_commit(commit_hash).await?;
                _raw.write()
                    .await
                    .create_semantic_commit(semantic_commit)
                    .await?;
            } else {
                let branches = _raw
                    .write()
                    .await
                    .get_branches(last_finalized_commit_hash)
                    .await?;

                for branch in branches {
                    let final_commit_hash =
                        _raw.write().await.locate_branch(branch.clone()).await?;
                    if final_commit_hash == parent_hash {
                        let semantic_commit =
                            _raw.write().await.read_semantic_commit(commit_hash).await?;
                        _raw.write()
                            .await
                            .create_semantic_commit(semantic_commit)
                            .await?;
                        break;
                    }
                }
            }

            todo!()
        }
    };

    todo!()
}
