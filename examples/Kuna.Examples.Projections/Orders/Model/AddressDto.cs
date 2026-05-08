#nullable disable
namespace Kuna.Examples.Projections.Orders.Model;

public class AddressDto
{
    public Guid Id { get; set; }

    public string Line1 { get; set; }

    public string Line2 { get; set; }

    public string State { get; set; }

    public string City { get; set; }

    public string PostCode { get; set; }

    public string Country { get; set; }

    public string GoogleLocationId { get; set; }

    public string GoogleLocationFormattedAddress { get; set; }

    public double? GoogleLatitude { get; set; }

    public double? GoogleLongitude { get; set; }

    public DateTimeOffset? GeocodeDateTime { get; set; }

    public bool? IsSystemGeocoded { get; set; }
}
