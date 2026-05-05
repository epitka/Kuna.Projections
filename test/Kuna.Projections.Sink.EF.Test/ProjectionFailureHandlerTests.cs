using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

public class ProjectionFailureHandlerTests
{
    [Fact]
    public async Task Handle_Should_Rethrow_When_DbContext_Is_Not_Registered()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var provider = services.BuildServiceProvider();
        var logger = provider.GetRequiredService<ILogger<ProjectionFailureHandler<TestModel, TestProjectionDbContext>>>();
        var handler = new ProjectionFailureHandler<TestModel, TestProjectionDbContext>(provider, new NoOpDuplicateKeyExceptionDetector(), logger);

        var failure = new ProjectionFailure(
            modelId: Guid.NewGuid(),
            eventNumber: 1,
            streamPosition: new GlobalEventPosition(10),
            failureCreatedOn: DateTime.UtcNow,
            exception: "boom",
            failureType: nameof(FailureType.EventProcessing),
            modelName: nameof(TestModel));

        var exception = await Should.ThrowAsync<InvalidOperationException>(() => handler.Handle(failure, CancellationToken.None));
        exception.Message.ShouldContain(nameof(TestProjectionDbContext));
    }

    private sealed class NoOpDuplicateKeyExceptionDetector : IDuplicateKeyExceptionDetector
    {
        public bool IsDuplicateKeyViolation(Exception exception)
        {
            return false;
        }
    }
}
