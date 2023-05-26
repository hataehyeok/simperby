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
                commit_hashes,
                branch_name: branch.clone(),
            };
            let message = Message::Branch(payload_branch);
            _dms.write().await.commit_message(&message).await?;
        } else if branch.starts_with("b-") {
            let b_commit_hash = _raw.write().await.locate_branch(branch.clone()).await?;
            let commit_hashes = _raw
                .write()
                .await
                .query_commit_path(b_commit_hash, last_finalized_commit_hash)
                .await?;
            let payload_branch = PayloadBranch {
                branch_type: BranchType::Block,
                commit_hashes,
                branch_name: branch.clone(),
            };
            let message = Message::Branch(payload_branch);
            _dms.write().await.commit_message(&message).await?;
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
pub async fn update(
    _raw: Arc<RwLock<RawRepository>>,
    _dms: Arc<RwLock<Dms<Message>>>,
) -> Result<(), Error> {
    // 1. dms에서부터 마지막 message
    let messages = _dms.write().await.read_messages().await?;
    let last_message = messages.last().unwrap().clone().message;

    // TODO: 아래 코드는 완전하지 않음.
    // update에서는 last_message에 dms에 있는 메세지를 다 불러온 다음에 그 타입에 따라서 Branch냐 Commit에 따라 나눠서 구현함.

    // 확인해야 할 부분 :
    // semantic commit을 만들어서 raw에 업데이트 하는 과정을 for문 안에서 create_semantic_commit을 하는 식으로 처리했는데,
    // 이렇게 했을 때 반영이 되는지 확인이 필요.
    //
    // 추가 구현 되어야 할 부분 :
    // branch를 만들 때, 해당 브랜치가 a-인지 b-인지 확인해서 각각 브랜치를 만들어야하는 부분이랑
    // message가 commit일 경우(Message::Commit)를 더 구현해야 함.

    match last_message {
        Message::Branch(payloadbranch) => {
            let commit_hashes = payloadbranch.commit_hashes;
            let branch_type = payloadbranch.branch_type;
            let branch_name = payloadbranch.branch_name;
            let raw = _raw.write().await;

            _raw.write()
                .await
                .checkout(FINALIZED_BRANCH_NAME.into())
                .await?;

            let last_finalized_commit_hash = _raw
                .write()
                .await
                .locate_branch(FINALIZED_BRANCH_NAME.into())
                .await?;

            // branch를 만들어야 함 그 전에 a-branch b-branch를 만들어야 할 듯
            _raw.write()
                .await
                .create_branch(branch_name.clone(), last_finalized_commit_hash)
                .await?;

            for commit in commit_hashes {
                // semantic commit을 만들어서 raw repo를 update
                // Question: 받아온 커밋들을 이렇게 적용하는 방식이 맞는가?
                _raw.write()
                    .await
                    .create_semantic_commit(semantic_commit)
                    .await?;
            }
        }
        Message::Commit(a) => {
            // hi
        }
    };

    todo!()
}
