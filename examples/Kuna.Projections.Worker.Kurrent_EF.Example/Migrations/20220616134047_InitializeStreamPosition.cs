using Kuna.Projections.Source.Kurrent.Extensions;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model;
using KurrentDB.Client;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Kuna.Projections.Worker.Kurrent_EF.Example.Migrations
{
    public partial class InitializeStreamPosition : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            var streamPosition = Position.Start.ToGlobalEventPosition();
            var modelName = nameof(Order);

            migrationBuilder.Sql($"Insert into dbo.CheckPoints (ModelName, StreamPosition) values ('{modelName}','{streamPosition}')");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("Delete from dbo.CheckPoints where ModelName='{modelName}'");
        }
    }
}
