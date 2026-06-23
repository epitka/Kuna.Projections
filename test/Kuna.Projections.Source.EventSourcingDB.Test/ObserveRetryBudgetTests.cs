using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public class ObserveRetryBudgetTests
{
    [Fact]
    public void New_Budget_Reports_No_Failures()
    {
        var budget = new ObserveRetryBudget(3);

        budget.ConsecutiveFailures.ShouldBe(0);
    }

    [Fact]
    public void Throws_When_Max_Is_Less_Than_One()
    {
        Should.Throw<ArgumentOutOfRangeException>(() => new ObserveRetryBudget(0));
    }

    [Fact]
    public void Exhausts_After_Max_Consecutive_Failures()
    {
        var budget = new ObserveRetryBudget(3);

        budget.RecordFailureAndCheckExhausted().ShouldBeFalse();
        budget.RecordFailureAndCheckExhausted().ShouldBeFalse();
        budget.RecordFailureAndCheckExhausted().ShouldBeTrue();
        budget.ConsecutiveFailures.ShouldBe(3);
    }

    [Fact]
    public void Success_Resets_The_Consecutive_Failure_Count()
    {
        var budget = new ObserveRetryBudget(3);

        budget.RecordFailureAndCheckExhausted();
        budget.RecordFailureAndCheckExhausted();
        budget.ConsecutiveFailures.ShouldBe(2);

        budget.RecordSuccess();

        budget.ConsecutiveFailures.ShouldBe(0);
        budget.RecordFailureAndCheckExhausted().ShouldBeFalse();
        budget.ConsecutiveFailures.ShouldBe(1);
    }

    [Fact]
    public void Interleaved_Successes_Prevent_Exhaustion_Despite_Many_Total_Failures()
    {
        // Regression: failures must be counted consecutively, not cumulatively, so a
        // long-running subscription that keeps recovering never terminates even after
        // far more than max total failures.
        var budget = new ObserveRetryBudget(3);

        for (var round = 0; round < 100; round++)
        {
            budget.RecordFailureAndCheckExhausted().ShouldBeFalse();
            budget.RecordFailureAndCheckExhausted().ShouldBeFalse();
            budget.RecordSuccess();
        }

        budget.ConsecutiveFailures.ShouldBe(0);
    }
}
