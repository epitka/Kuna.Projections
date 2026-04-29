using Kuna.Projections.Source.Kurrent;
using KurrentDB.Client;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

public class KurrentDbSubscriptionFilterFactoryTests
{
    public static TheoryData<KurrentDbFilterSettings> CreateCases()
    {
        return
        [
            new KurrentDbFilterSettings
            {
                Kind = KurrentDbFilterKind.StreamPrefix,
                Prefixes = ["orders-", "payments-",],
            },
            new KurrentDbFilterSettings
            {
                Kind = KurrentDbFilterKind.StreamRegex,
                Regex = "^order|^invoice",
            },
            new KurrentDbFilterSettings
            {
                Kind = KurrentDbFilterKind.EventTypePrefix,
                Prefixes = ["Order", "Invoice",],
            },
            new KurrentDbFilterSettings
            {
                Kind = KurrentDbFilterKind.EventTypeRegex,
                Regex = "^Order|^Invoice",
            },
        ];
    }
    [Fact]
    public void GetPrefixes_Should_Return_All_Configured_Prefixes()
    {
        var settings = new KurrentDbFilterSettings
        {
            Kind = KurrentDbFilterKind.StreamPrefix,
            Prefixes = ["orders-", "payments-",],
        };

        KurrentDbSubscriptionFilterFactory.GetPrefixes(settings).ShouldBe(["orders-", "payments-",]);
    }

    [Fact]
    public void GetPrefixes_Should_Throw_When_No_Prefixes_Are_Configured()
    {
        var settings = new KurrentDbFilterSettings
        {
            Kind = KurrentDbFilterKind.StreamPrefix,
            Prefixes = [],
        };

        var ex = Should.Throw<InvalidOperationException>(() => KurrentDbSubscriptionFilterFactory.GetPrefixes(settings));
        ex.Message.ShouldContain("at least one non-empty prefix");
    }

    [Fact]
    public void GetPrefixes_Should_Throw_When_Any_Prefix_Is_Empty()
    {
        var settings = new KurrentDbFilterSettings
        {
            Kind = KurrentDbFilterKind.EventTypePrefix,
            Prefixes = ["Order", "",],
        };

        var ex = Should.Throw<InvalidOperationException>(() => KurrentDbSubscriptionFilterFactory.GetPrefixes(settings));
        ex.Message.ShouldContain("at least one non-empty prefix");
    }

    [Fact]
    public void GetRegex_Should_Return_Configured_Regex()
    {
        var settings = new KurrentDbFilterSettings
        {
            Kind = KurrentDbFilterKind.StreamRegex,
            Regex = "^order|^invoice",
        };

        KurrentDbSubscriptionFilterFactory.GetRegex(settings).ShouldBe("^order|^invoice");
    }

    [Fact]
    public void GetRegex_Should_Throw_When_Regex_Is_Missing()
    {
        var settings = new KurrentDbFilterSettings
        {
            Kind = KurrentDbFilterKind.EventTypeRegex,
            Regex = string.Empty,
        };

        var ex = Should.Throw<InvalidOperationException>(() => KurrentDbSubscriptionFilterFactory.GetRegex(settings));
        ex.Message.ShouldContain("non-empty regular expression");
    }

    [Theory]
    [MemberData(nameof(CreateCases))]
    public void Create_Should_Succeed_For_Supported_Filter_Kinds(KurrentDbFilterSettings settings)
    {
        var options = KurrentDbSubscriptionFilterFactory.Create(settings);

        options.ShouldNotBeNull();
        options.ShouldBeOfType<SubscriptionFilterOptions>();
    }
}
