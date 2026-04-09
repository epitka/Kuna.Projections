using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Kuna.Projections.Sink.EF;

/// <summary>
/// Persists projection-processing failures through EF Core and marks affected
/// model rows as stream-faulted when possible.
/// </summary>
public class ProjectionFailureHandler<TState, TDataContext>
    : IProjectionFailureHandler<TState>
    where TState : class, IModel, new()
    where TDataContext : DbContext
{
    private const int DuplicatePkViolationError = 2627;
    private const string PostgresDuplicatePkViolationError = PostgresErrorCodes.UniqueViolation;
    private const int MaxExceptionMessageLength = 4000;
    private const int MaxExceptionMessageLengthAllowed = MaxExceptionMessageLength - 1;

    private readonly IServiceProvider serviceProvider;
    private readonly ILogger logger;

    /// <summary>
    /// Initializes the failure handler with the service provider used to create
    /// short-lived EF DbContext scopes.
    /// </summary>
    public ProjectionFailureHandler(
        IServiceProvider serviceProvider,
        ILogger<ProjectionFailureHandler<TState, TDataContext>> logger)
    {
        this.serviceProvider = serviceProvider;
        this.logger = logger;
    }

    /// <summary>
    /// Persists a projection failure record for the specified model and updates
    /// the stored model state to indicate stream-processing failure.
    /// </summary>
    public async Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        try
        {
            using var serviceScope = this.serviceProvider.CreateScope();

            await using var dbContext = serviceScope
                                        .ServiceProvider
                                        .GetRequiredService<TDataContext>();

            var dbModel = await dbContext.FindAsync<TState>([failure.ModelId,], cancellationToken);

            if (dbModel is { HasStreamProcessingFaulted: false, })
            {
                dbModel.HasStreamProcessingFaulted = true;
            }

            dbContext.Attach(failure);
            dbContext.Entry(failure).State = EntityState.Added;

            await dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException dex) when (dex.InnerException is SqlException { Number: DuplicatePkViolationError, })
        {
            await this.FindAndUpdateFailure(failure, cancellationToken);
        }
        catch (DbUpdateException dex) when (dex.InnerException is PostgresException { SqlState: PostgresDuplicatePkViolationError, })
        {
            await this.FindAndUpdateFailure(failure, cancellationToken);
        }
        catch (SqlException sx) when (sx.Number == DuplicatePkViolationError)
        {
            await this.FindAndUpdateFailure(failure, cancellationToken);
        }
        catch (PostgresException px) when (px.SqlState == PostgresDuplicatePkViolationError)
        {
            await this.FindAndUpdateFailure(failure, cancellationToken);
        }
        catch (Exception e)
        {
            this.logger.LogError(
                "Failed to persist {@Failure} for {Model} {ModelId}, exception {Exception}",
                failure,
                failure.ModelName,
                failure.ModelId,
                e.ToString());

            throw;
        }
    }

    private async Task FindAndUpdateFailure(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        using var serviceScope = this.serviceProvider.CreateScope();

        await using var dbContext = serviceScope
                                    .ServiceProvider
                                    .GetRequiredService<TDataContext>();

        var eventFailure = await dbContext.FindAsync<ProjectionFailure>(
                               [failure.ModelName, failure.ModelId,],
                               cancellationToken);

        if (eventFailure != null)
        {
            eventFailure.Exception += " Additional_Failure: " + failure.Exception;

            if (eventFailure.Exception.Length >= MaxExceptionMessageLength)
            {
                eventFailure.Exception = eventFailure.Exception.Substring(0, MaxExceptionMessageLengthAllowed);
            }

            await dbContext.SaveChangesAsync(cancellationToken);
        }
    }
}
