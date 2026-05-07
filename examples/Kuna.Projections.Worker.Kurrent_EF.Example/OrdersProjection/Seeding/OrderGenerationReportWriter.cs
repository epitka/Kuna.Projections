namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Seeding;

public static class OrderGenerationReportWriter
{
    public static async Task WriteAsync(OrderGenerationReport report, string path)
    {
        var directory = Path.GetDirectoryName(path);

        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var file = File.Create(path);
        await System.Text.Json.JsonSerializer.SerializeAsync(
            file,
            report,
            new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web)
            {
                WriteIndented = true,
            });

        await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes(Environment.NewLine));
    }
}
