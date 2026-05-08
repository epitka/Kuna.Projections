using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Kuna.Projections.Worker.Kurrent_EF.Example.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "dbo");

            migrationBuilder.CreateTable(
                name: "CheckPoints",
                schema: "dbo",
                columns: table => new
                {
                    ModelName = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: false),
                    GlobalEventPosition = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CheckPoints", x => x.ModelName);
                });

            migrationBuilder.CreateTable(
                name: "Orders",
                schema: "dbo",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    Amount = table.Column<decimal>(type: "numeric(18,2)", nullable: false),
                    TaxAmount = table.Column<decimal>(type: "numeric(18,2)", nullable: true),
                    ShippingAmount = table.Column<decimal>(type: "numeric(18,2)", nullable: true),
                    MerchantTransactionFeeAmount = table.Column<decimal>(type: "numeric", nullable: true),
                    MerchantTransactionFeePercent = table.Column<decimal>(type: "numeric", nullable: true),
                    MerchantTransactionFeePercentCalculated = table.Column<decimal>(type: "numeric", nullable: false),
                    OrderNumber = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ShippingAddress_Line1 = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: true),
                    ShippingAddress_Line2 = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: true),
                    ShippingAddress_City = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ShippingAddress_State = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    ShippingAddress_PostCode = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    ShippingAddress_Country = table.Column<string>(type: "character varying(25)", maxLength: 25, nullable: true),
                    BillingAddress_Line1 = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: true),
                    BillingAddress_Line2 = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: true),
                    BillingAddress_City = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    BillingAddress_State = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    BillingAddress_PostCode = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    BillingAddress_Country = table.Column<string>(type: "character varying(25)", maxLength: 25, nullable: true),
                    Customer_FirstName = table.Column<string>(type: "character varying(150)", maxLength: 150, nullable: true),
                    Customer_LastName = table.Column<string>(type: "character varying(150)", maxLength: 150, nullable: true),
                    Customer_Email = table.Column<string>(type: "character varying(250)", maxLength: 250, nullable: true),
                    Customer_PhoneNumber = table.Column<string>(type: "character varying(50)", maxLength: 50, nullable: true),
                    OrderStatus = table.Column<int>(type: "integer", nullable: false),
                    CreatedDateTime = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    CompletedDateTime = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    CustomerId = table.Column<Guid>(type: "uuid", nullable: true),
                    MerchantId = table.Column<Guid>(type: "uuid", nullable: false),
                    TotalFundsCaptured = table.Column<decimal>(type: "numeric(18,2)", nullable: false),
                    TotalFundsVoided = table.Column<decimal>(type: "numeric(18,2)", nullable: false),
                    TotalFundsRefunded = table.Column<decimal>(type: "numeric(18,2)", nullable: false),
                    Source = table.Column<string>(type: "character varying(30)", maxLength: 30, nullable: true),
                    MerchantPlatformId = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    CurrencyCode = table.Column<string>(type: "character varying(3)", maxLength: 3, nullable: true),
                    MerchantReference = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    PaymentAuthorizationId = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    CaptureReferences = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    FeeReferences = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    VoidReferences = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    EventNumber = table.Column<long>(type: "bigint", nullable: true),
                    GlobalEventPosition = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: false),
                    HasStreamProcessingFaulted = table.Column<bool>(type: "boolean", nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Orders", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "ProjectionFailures",
                schema: "dbo",
                columns: table => new
                {
                    ModelId = table.Column<Guid>(type: "uuid", nullable: false),
                    ModelName = table.Column<string>(type: "text", nullable: false),
                    EventNumber = table.Column<long>(type: "bigint", nullable: false),
                    GlobalEventPosition = table.Column<string>(type: "character varying(128)", maxLength: 128, nullable: false),
                    FailureCreatedOn = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    Exception = table.Column<string>(type: "text", nullable: false),
                    FailureType = table.Column<string>(type: "text", nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ProjectionFailures", x => new { x.ModelName, x.ModelId });
                });

            migrationBuilder.CreateTable(
                name: "OrderRefunds",
                schema: "dbo",
                columns: table => new
                {
                    Id = table.Column<Guid>(type: "uuid", nullable: false),
                    OrderId = table.Column<Guid>(type: "uuid", nullable: false),
                    Amount = table.Column<decimal>(type: "numeric(18,2)", precision: 18, scale: 2, nullable: false),
                    RefundId = table.Column<Guid>(type: "uuid", nullable: false),
                    MerchantId = table.Column<Guid>(type: "uuid", nullable: false),
                    MerchantReference = table.Column<string>(type: "character varying(100)", maxLength: 100, nullable: true),
                    MerchantRefundFeeRebate = table.Column<decimal>(type: "numeric", nullable: false),
                    MerchantRefundFeeRebatePercent = table.Column<decimal>(type: "numeric", nullable: false),
                    MerchantRefundTransactionFee = table.Column<decimal>(type: "numeric", nullable: false),
                    RefundDateTime = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_OrderRefunds", x => x.Id);
                    table.ForeignKey(
                        name: "FK_OrderRefunds_Orders_OrderId",
                        column: x => x.OrderId,
                        principalSchema: "dbo",
                        principalTable: "Orders",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_OrderRefunds_OrderId",
                schema: "dbo",
                table: "OrderRefunds",
                column: "OrderId");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_CustomerId",
                schema: "dbo",
                table: "Orders",
                column: "CustomerId");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_EventNumber",
                schema: "dbo",
                table: "Orders",
                column: "EventNumber");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_MerchantId",
                schema: "dbo",
                table: "Orders",
                column: "MerchantId");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_MerchantPlatformId",
                schema: "dbo",
                table: "Orders",
                column: "MerchantPlatformId");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_MerchantReference",
                schema: "dbo",
                table: "Orders",
                column: "MerchantReference");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_OrderNumber",
                schema: "dbo",
                table: "Orders",
                column: "OrderNumber");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_OrderStatus",
                schema: "dbo",
                table: "Orders",
                column: "OrderStatus");

            migrationBuilder.CreateIndex(
                name: "IX_Orders_PaymentAuthorizationId",
                schema: "dbo",
                table: "Orders",
                column: "PaymentAuthorizationId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "CheckPoints",
                schema: "dbo");

            migrationBuilder.DropTable(
                name: "OrderRefunds",
                schema: "dbo");

            migrationBuilder.DropTable(
                name: "ProjectionFailures",
                schema: "dbo");

            migrationBuilder.DropTable(
                name: "Orders",
                schema: "dbo");
        }
    }
}
