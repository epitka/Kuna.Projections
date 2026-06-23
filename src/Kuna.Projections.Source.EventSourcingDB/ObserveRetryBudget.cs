namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Tracks <em>consecutive</em> failures of the observe subscription so the source
/// gives up only after a run of uninterrupted failures, not after that many
/// failures spread across the lifetime of the process. Any successfully observed
/// event resets the count, so transient disconnects that each recover do not
/// accumulate toward the limit.
/// </summary>
internal sealed class ObserveRetryBudget
{
    private readonly int maxConsecutiveFailures;
    private int consecutiveFailures;

    /// <summary>
    /// Initializes the budget with the maximum number of consecutive failures
    /// tolerated before the subscription is considered exhausted.
    /// </summary>
    public ObserveRetryBudget(int maxConsecutiveFailures)
    {
        if (maxConsecutiveFailures < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxConsecutiveFailures),
                maxConsecutiveFailures,
                "The maximum number of consecutive failures must be at least 1.");
        }

        this.maxConsecutiveFailures = maxConsecutiveFailures;
    }

    /// <summary>
    /// Gets the number of failures observed since the last success.
    /// </summary>
    public int ConsecutiveFailures => this.consecutiveFailures;

    /// <summary>
    /// Resets the consecutive-failure count after a successfully observed event.
    /// </summary>
    public void RecordSuccess()
    {
        this.consecutiveFailures = 0;
    }

    /// <summary>
    /// Records a failure and reports whether the budget is now exhausted, i.e.
    /// whether the configured number of consecutive failures has been reached.
    /// </summary>
    public bool RecordFailureAndCheckExhausted()
    {
        this.consecutiveFailures++;

        return this.consecutiveFailures >= this.maxConsecutiveFailures;
    }
}
