Ppopgi Finalizer Bot is an off-chain automation service responsible for monitoring lottery states and triggering permissionless lifecycle transitions when conditions are met.

The bot watches newly deployed lotteries through the registry and subgraph, tracking funding confirmations, ticket sales progress, deadline expiration and randomness states. When a lottery becomes eligible for drawing or requires recovery actions, the bot submits the appropriate on-chain transaction to finalize the draw or cancel stalled lotteries.

Its responsibilities include:
- Detecting lotteries ready for randomness requests
- Triggering draw finalization once conditions are satisfied
- Monitoring entropy callbacks and stalled randomness windows
- Executing safety cancellations after configured timeouts
- Ensuring inactive lotteries do not remain stuck in intermediate states

The bot operates under a **non-custodial and permissionless model**:
- It holds no protocol funds
- It has no privileged permissions over lotteries
- Any user could perform the same actions manually
- It simply improves UX and liveness guarantees

By automating lifecycle progression, the finalizer bot helps maintain a responsive protocol while preserving the trustless nature and security assumptions of the underlying smart contracts.