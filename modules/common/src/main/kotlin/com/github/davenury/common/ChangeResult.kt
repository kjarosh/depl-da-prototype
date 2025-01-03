package com.github.davenury.common

data class ChangeResult(
    val status: Status,
    val detailedMessage: String? = null,
    /**
     * For successful results, it's the ID of the newly added entry.
     * For unsuccessful results, it's the current entry ID,
     * e.g. the one that conflicted with the change.
     */
    val entryId: String? = null,
    val currentConsensusLeader: PeerId? = null,
) {
    enum class Status {
        /**
         * Change accepted and applied
         */
        SUCCESS,

        /**
         * Change not applied due to another change conflicting with it.
         */
        CONFLICT,

        /**
         * Change not applied due to a timeout.
         * For instance, there was not enough peers to accept the change
         * within the time limit.
         */
        TIMEOUT,

        /**
         * Change was not applied due to invalid parent id - protocol was not used
         */
        REJECTED,

        /**
         * Change was applied with ABORT result
         */
        ABORTED,
    }

    fun assertSuccess() {
        if (status != Status.SUCCESS) {
            throw AssertionError("ChangeResult: Expected success, got $this")
        }
    }
}
