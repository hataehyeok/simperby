use vetomint::*;

/// A very normal, desirable, and expected scenario.
#[test]
fn normal_2() {
    let mut height_info = HeightInfo {
        validators: vec![2, 2, 2, 2, 2],
        this_node_index: Some(0),
        timestamp: 0,
        consensus_params: ConsensusParams {
            timeout_ms: 100,
            repeat_round_for_first_leader: 1,
        },
        initial_block_candidate: 0,
    };
    let mut proposer = Vetomint::new(height_info.clone());
    let mut nodes = Vec::new();
    for i in 1..=4 {
        height_info.this_node_index = Some(i);
        nodes.push(Vetomint::new(height_info.clone()));
    }
    let response = proposer.progress(ConsensusEvent::Start, 0);
    assert_eq!(
        response,
        vec![
            ConsensusResponse::BroadcastProposal {
                proposal: 0,
                valid_round: None,
                round: 0,
            },
            ConsensusResponse::BroadcastPrevote {
                proposal: Some(0),
                round: 0
            }
        ]
    );
    for node in nodes.iter_mut() {
        let response = node.progress(ConsensusEvent::Start, 0);
        assert_eq!(response, vec![]);
    }

    for node in nodes.iter_mut() {
        let response = node.progress(
            ConsensusEvent::BlockProposalReceived {
                proposal: 0,
                valid: true,
                valid_round: None,
                proposer: 0,
                round: 0,
                favor: true,
            },
            1,
        );
        assert_eq!(
            response,
            vec![ConsensusResponse::BroadcastPrevote {
                proposal: Some(0),
                round: 0,
            }]
        );
    }

    let mut nodes = vec![vec![proposer], nodes].concat();

    for (i, node) in nodes.iter_mut().enumerate() {
        let response = node.progress(
            ConsensusEvent::Prevote {
                proposal: Some(0),
                signer: (i + 1) % 5,
                round: 0,
            },
            2,
        );
        assert_eq!(response, Vec::new());
        let response = node.progress(
            ConsensusEvent::Prevote {
                proposal: Some(0),
                signer: (i + 2) % 5,
                round: 0,
            },
            2,
        );
        assert_eq!(response, Vec::new());
        let response = node.progress(
            ConsensusEvent::Prevote {
                proposal: Some(0),
                signer: (i + 3) % 5,
                round: 0,
            },
            2,
        );
        assert_eq!(
            response,
            vec![ConsensusResponse::BroadcastPrecommit {
                proposal: Some(0),
                round: 0,
            }]
        );
        let response = node.progress(
            ConsensusEvent::Prevote {
                proposal: Some(0),
                signer: (i + 4) % 5,
                round: 0,
            },
            2,
        );
        assert_eq!(response, Vec::new());
    }

    for (i, node) in nodes.iter_mut().enumerate() {
        let response = node.progress(
            ConsensusEvent::Precommit {
                proposal: Some(0),
                signer: (i + 1) % 5,
                round: 0,
            },
            3,
        );
        assert_eq!(response, Vec::new());
        let response = node.progress(
            ConsensusEvent::Precommit {
                proposal: Some(0),
                signer: (i + 2) % 5,
                round: 0,
            },
            3,
        );
        assert_eq!(response, Vec::new());
        let response = node.progress(
            ConsensusEvent::Precommit {
                proposal: Some(0),
                signer: (i + 3) % 5,
                round: 0,
            },
            3,
        );
        assert_eq!(
            response,
            vec![ConsensusResponse::FinalizeBlock {
                proposal: 0,
                proof: (0..5).into_iter().filter(|x| *x != (i + 4) % 5).collect(),
            }]
        );
    }
}
