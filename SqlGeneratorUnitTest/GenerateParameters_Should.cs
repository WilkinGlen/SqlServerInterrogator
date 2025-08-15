namespace SqlGeneratorUnitTest;

using FluentAssertions;
using SqlServerInterrogator.Services;

public sealed class GenerateParameters_Should
{
    [Fact]
    public void ReturnEmpty_WhenNoParameters()
    {
        var sql = "SELECT * FROM Users";
        var parameters = new Dictionary<string, string>();

        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);

        _ = generatedParameters.Should().BeEmpty();
    }

    [Fact]
    public void ReturnCorrectParameters_WhenStringParameterProvided()
    {
        var sql = "SELECT * FROM Users WHERE Name = @Name";
        var parameters = new Dictionary<string, string>
        {
            { "Name", "John Doe" }
        };

        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);

        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@Name");
        _ = generatedParameters["@Name"].Should().Be("John Doe");
    }

    [Fact]
    public void ReturnCorrectParameters_WhenIntParameterProvided()
    {
        var sql = "SELECT * FROM Users WHERE Id = @Id AND Age = @Age";
        var parameters = new Dictionary<string, string>
        {
            { "Id", "123" },
            { "Age", "30" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(2);
        _ = generatedParameters.Should().ContainKey("@Id");
        _ = generatedParameters["@Id"].Should().Be(123);
        _ = generatedParameters.Should().ContainKey("@Age");
        _ = generatedParameters["@Age"].Should().Be(30);
    }

    [Fact]
    public void ReturnCorrectParameters_WhenDateTimeParameterProvided()
    {
        var sql = "SELECT * FROM Users WHERE CreatedAt = @CreatedAt";
        var parameters = new Dictionary<string, string>
        {
            { "CreatedAt", "2023-10-01T12:00:00" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@CreatedAt");
        _ = generatedParameters["@CreatedAt"].Should().Be(new DateTime(2023, 10, 1, 12, 0, 0));
    }

    [Fact]
    public void ReturnCorrectParameters_WhenBooleanParameterProvided()
    {
        var sql = "SELECT * FROM Users WHERE IsActive = @IsActive";
        var parameters = new Dictionary<string, string>
        {
            { "IsActive", "true" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@IsActive");
        _ = generatedParameters["@IsActive"].Should().Be(true);
    }

    [Fact]
    public void ReturnCorrectParameters_WhenDecimalParameterProvided()
    {
        var sql = "SELECT * FROM Orders WHERE Price = @Price";
        var parameters = new Dictionary<string, string>
        {
            { "Price", "99.99" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@Price");
        _ = generatedParameters["@Price"].Should().Be(99.99m);
    }

    [Fact]
    public void ReturnCorrectParameters_WhenFloatParameterProvided()
    {
        var sql = "SELECT * FROM Measurements WHERE Temperature = @Temperature";
        var parameters = new Dictionary<string, string>
        {
            { "Temperature", "36.6" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@Temperature");
        _ = generatedParameters["@Temperature"].Should().Be(36.6f);
    }

    [Fact]
    public void ReturnCorrectParameters_WhenDoubleParameterProvided()
    {
        var sql = "SELECT * FROM Calculations WHERE Result = @Result";
        var parameters = new Dictionary<string, string>
        {
            { "Result", "123.456789" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@Result");
        _ = generatedParameters["@Result"].Should().Be(123.456789);
    }

    [Fact]
    public void ReturnCorrectParameters_WhenGuidParameterProvided()
    {
        var guid = "550e8400-e29b-41d4-a716-446655440000";
        var sql = "SELECT * FROM Users WHERE GlobalId = @GlobalId";
        var parameters = new Dictionary<string, string>
        {
            { "GlobalId", guid }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(1);
        _ = generatedParameters.Should().ContainKey("@GlobalId");
        _ = generatedParameters["@GlobalId"].Should().Be(Guid.Parse(guid));
    }

    [Fact]
    public void ReturnCorrectParameters_WhenAllPossibleTypesOfParametersProvided()
    {
        var sql = "SELECT * FROM MixedTypes WHERE StringParam = @StringParam AND IntParam = @IntParam AND DateTimeParam = @DateTimeParam AND BoolParam = @BoolParam AND DecimalParam = @DecimalParam AND FloatParam = @FloatParam AND DoubleParam = @DoubleParam AND GuidParam = @GuidParam";
        var parameters = new Dictionary<string, string>
        {
            { "StringParam", "Test" },
            { "IntParam", "42" },
            { "DateTimeParam", "2023-10-01T12:00:00" },
            { "BoolParam", "true" },
            { "DecimalParam", "99.99" },
            { "FloatParam", "36.6" },
            { "DoubleParam", "123.456789" },
            { "GuidParam", "550e8400-e29b-41d4-a716-446655440000" }
        };
        var generatedParameters = SqlGenerator.GenerateParameters(sql, parameters);
        _ = generatedParameters.Should().HaveCount(8);
        _ = generatedParameters["@StringParam"].Should().Be("Test");
        _ = generatedParameters["@IntParam"].Should().Be(42);
        _ = generatedParameters["@DateTimeParam"].Should().Be(new DateTime(2023, 10, 1, 12, 0, 0));
        _ = generatedParameters["@BoolParam"].Should().Be(true);
        _ = generatedParameters["@DecimalParam"].Should().Be(99.99m);
        _ = generatedParameters["@FloatParam"].Should().Be(36.6f);
        _ = generatedParameters["@DoubleParam"].Should().Be(123.456789);
        _ = generatedParameters["@GuidParam"].Should().Be(Guid.Parse("550e8400-e29b-41d4-a716-446655440000"));
    }
}
