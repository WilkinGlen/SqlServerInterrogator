namespace SqlGeneratorUnitTest;

using FluentAssertions;
using SqlServerInterrogator.Models;
using SqlServerInterrogator.Services;

public class GenerateSelectStatement_Should
{
    [Fact]
    public void GenerateCorrectSql_WhenAllColumnsToSelectAreInSameTable()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1], [TestTable].[Column2]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables =
            [
                new TableInfo
                {
                    TableId = 1,
                    Name = "TestTable",
                    Columns =
                    [
                        new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 },
                        new ColumnInfo { ColumnId = 2, Name = "Column2", TableId = 1 }
                    ]
                }
            ]
        };

        var columnsToSelect = databaseInfo.Tables[0].Columns;

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void GenerateCorrectSql_WhenColumnsToSelectAreInDifferentTables_ThatCanBeJoinedDirectly()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1], [Table2].[Column2]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key] = [Table2].[ForeignKey]\r\n";

        var table1 = new TableInfo
        {
            TableId = 1,
            Name = "Table1",
            Columns =
            [
                new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 },
                new ColumnInfo { ColumnId = 2, Name = "Key", TableId = 1 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 1,
                    Name = "PrimaryKey",
                    TableId = 1,
                    SourceColumnName = "Key",
                    IsForeignKey = false
                }
            ]
        };
        var table2 = new TableInfo
        {
            TableId = 2,
            Name = "Table2",
            Columns =
            [
                new ColumnInfo { ColumnId = 3, Name = "Column2", TableId = 2 },
                new ColumnInfo { ColumnId = 4, Name = "ForeignKey", TableId = 2 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 2,
                    Name = "ForeignKey",
                    TableId = 2,
                    IsForeignKey = true,
                    SourceColumnName = "ForeignKey",
                    ReferencedTableName = "Table1",
                    ReferencedColumnName = "Key"
                }
            ]
        };

        // Set up the join relationships
        table1.TablesICanJoinTo = [table2];
        table2.TablesICanJoinTo = [table1];

        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = [table1, table2]
        };

        var columnsToSelect = new List<ColumnInfo>
        {
            table1.Columns[0],  // Column1 from Table1
            table2.Columns[0]   // Column2 from Table2
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void GenerateCorrectSql_WhenColumnsToSelectAreInDifferentTables_ThatMustBeJoinedIndirectly()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1], [Table3].[Column3]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key1] = [Table2].[ForeignKey1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table3] AS [Table3]\r\n    ON [Table2].[ForeignKey3] = [Table3].[Key3]\r\n";

        var table1 = new TableInfo
        {
            TableId = 1,
            Name = "Table1",
            Columns =
            [
                new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 },
                new ColumnInfo { ColumnId = 2, Name = "Key1", TableId = 1 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 1,
                    Name = "PK_Table1",
                    TableId = 1,
                    SourceColumnName = "Key1",
                    IsForeignKey = false
                }
            ]
        };
        var table3 = new TableInfo
        {
            TableId = 3,
            Name = "Table3",
            Columns =
            [
                new ColumnInfo { ColumnId = 5, Name = "Column3", TableId = 3 },
                new ColumnInfo { ColumnId = 6, Name = "Key3", TableId = 3 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 3,
                    Name = "PK_Table3",
                    TableId = 3,
                    SourceColumnName = "Key3",
                    IsForeignKey = false
                }
            ]
        };
        var table2 = new TableInfo
        {
            TableId = 2,
            Name = "Table2",
            Columns =
            [
                new ColumnInfo { ColumnId = 3, Name = "ForeignKey1", TableId = 2 },
                new ColumnInfo { ColumnId = 4, Name = "ForeignKey3", TableId = 2 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 2,
                    Name = "FK_Table2_Table1",
                    TableId = 2,
                    IsForeignKey = true,
                    SourceColumnName = "ForeignKey1",
                    ReferencedTableName = "Table1",
                    ReferencedColumnName = "Key1"
                },
                new KeyInfo
                {
                    KeyId = 4,
                    Name = "FK_Table2_Table3",
                    TableId = 2,
                    IsForeignKey = true,
                    SourceColumnName = "ForeignKey3",
                    ReferencedTableName = "Table3",
                    ReferencedColumnName = "Key3"
                }
            ]
        };

        // Set up the join relationships
        table1.TablesICanJoinTo = [table2];
        table2.TablesICanJoinTo = [table1, table3];
        table3.TablesICanJoinTo = [table2];

        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = [table1, table2, table3]
        };

        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0],  // Column1 from Table1
            databaseInfo.Tables[2].Columns[0]   // Column3 from Table3
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void GenerateCorrectSql_WhenColumnsToSelectAreInDifferentTables_AndSomeTablesCanBeDirectlyJoinedAndSomeMustBeInDirectlyJoined()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1], [Table2].[Column2], [Table4].[Column4]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key1] = [Table2].[ForeignKey1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table3] AS [Table3]\r\n    ON [Table2].[Key2] = [Table3].[ForeignKey2]\r\nLEFT JOIN [TestDatabase].[dbo].[Table4] AS [Table4]\r\n    ON [Table3].[Key3] = [Table4].[ForeignKey3]\r\n";

        var table1 = new TableInfo
        {
            TableId = 1,
            Name = "Table1",
            Columns =
            [
                new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 },
                new ColumnInfo { ColumnId = 2, Name = "Key1", TableId = 1 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 1,
                    Name = "PK_Table1",
                    TableId = 1,
                    SourceColumnName = "Key1",
                    IsForeignKey = false
                }
            ]
        };
        var table2 = new TableInfo
        {
            TableId = 2,
            Name = "Table2",
            Columns =
            [
                new ColumnInfo { ColumnId = 3, Name = "Column2", TableId = 2 },
                new ColumnInfo { ColumnId = 4, Name = "ForeignKey1", TableId = 2 },
                new ColumnInfo { ColumnId = 5, Name = "Key2", TableId = 2 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 2,
                    Name = "FK_Table2_Table1",
                    TableId = 2,
                    IsForeignKey = true,
                    SourceColumnName = "ForeignKey1",
                    ReferencedTableName = "Table1",
                    ReferencedColumnName = "Key1"
                },
                new KeyInfo
                {
                    KeyId = 3,
                    Name = "PK_Table2",
                    TableId = 2,
                    SourceColumnName = "Key2",
                    IsForeignKey = false
                }
            ]
        };
        var table3 = new TableInfo
        {
            TableId = 3,
            Name = "Table3",
            Columns =
            [
                new ColumnInfo { ColumnId = 6, Name = "ForeignKey2", TableId = 3 },
                new ColumnInfo { ColumnId = 7, Name = "Key3", TableId = 3 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 4,
                    Name = "FK_Table3_Table2",
                    TableId = 3,
                    IsForeignKey = true,
                    SourceColumnName = "ForeignKey2",
                    ReferencedTableName = "Table2",
                    ReferencedColumnName = "Key2"
                },
                new KeyInfo
                {
                    KeyId = 5,
                    Name = "PK_Table3",
                    TableId = 3,
                    SourceColumnName = "Key3",
                    IsForeignKey = false
                }
            ]
        };
        var table4 = new TableInfo
        {
            TableId = 4,
            Name = "Table4",
            Columns =
            [
                new ColumnInfo { ColumnId = 8, Name = "Column4", TableId = 4 },
                new ColumnInfo { ColumnId = 9, Name = "ForeignKey3", TableId = 4 }
            ],
            Keys =
            [
                new KeyInfo
                {
                    KeyId = 6,
                    Name = "FK_Table4_Table3",
                    TableId = 4,
                    IsForeignKey = true,
                    SourceColumnName = "ForeignKey3",
                    ReferencedTableName = "Table3",
                    ReferencedColumnName = "Key3"
                }
            ]
        };

        // Set up the join relationships
        table1.TablesICanJoinTo = [table2];
        table2.TablesICanJoinTo = [table1, table3];
        table3.TablesICanJoinTo = [table2, table4];
        table4.TablesICanJoinTo = [table3];

        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = [table1, table2, table3, table4]
        };

        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0],  // Column1 from Table1
            databaseInfo.Tables[1].Columns[0],  // Column2 from Table2
            databaseInfo.Tables[3].Columns[0]   // Column4 from Table4
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void ThrowArgumentException_WhenNoColumnsToSelectProvided()
    {
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = []
        };

        var action = () => SqlGenerator.GenerateSelectStatement([], databaseInfo);

        _ = action.Should()
            .Throw<ArgumentException>()
            .WithMessage("Columns and database information must be provided.");
    }

    [Fact]
    public void ThrowArgumentException_WhenColumnsToSelectIsNull()
    {
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = []
        };

        var action = () => SqlGenerator.GenerateSelectStatement(null!, databaseInfo);

        _ = action.Should()
            .Throw<ArgumentException>()
            .WithMessage("Columns and database information must be provided.");
    }

    [Fact]
    public void ThrowArgumentException_WhenDatabaseInfoIsNull()
    {
        var columns = new List<ColumnInfo>
        {
            new() { ColumnId = 1, Name = "Column1", TableId = 1 }
        };

        var action = () => SqlGenerator.GenerateSelectStatement(columns, null!);

        _ = action.Should()
            .Throw<ArgumentException>()
            .WithMessage("Columns and database information must be provided.");
    }

    [Fact]
    public void ThrowInvalidOperationException_WhenNoJoinPathExists()
    {
        var table1 = new TableInfo
        {
            TableId = 1,
            Name = "Table1",
            Columns =
            [
                new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 }
            ],
            Keys = []
        };
        var table2 = new TableInfo
        {
            TableId = 2,
            Name = "Table2",
            Columns =
            [
                new ColumnInfo { ColumnId = 2, Name = "Column2", TableId = 2 }
            ],
            Keys = []
        };

        // No join relationships set up between tables
        table1.TablesICanJoinTo = [];
        table2.TablesICanJoinTo = [];

        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = [table1, table2]
        };

        var columnsToSelect = new List<ColumnInfo>
        {
            table1.Columns[0],  // Column1 from Table1
            table2.Columns[0]   // Column2 from Table2
        };

        var action = () => SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = action.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("No join path found between tables Table1 and Table2");
    }

    [Fact]
    public void HandleNullColumns_InColumnList()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables =
            [
                new TableInfo
                {
                    TableId = 1,
                    Name = "TestTable",
                    Columns =
                    [
                        new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 }
                    ],
                    Keys = []
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0],
            null!
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleDuplicateColumns_InColumnList()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1], [TestTable].[Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables =
            [
                new TableInfo
                {
                    TableId = 1,
                    Name = "TestTable",
                    Columns =
                    [
                        new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 }
                    ],
                    Keys = []
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0],
            databaseInfo.Tables[0].Columns[0]  // Same column twice
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleEmptyTables_InDatabaseInfo()
    {
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = []
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            new() { ColumnId = 1, Name = "Column1", TableId = 1 }
        };

        var action = () => SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = action.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ThrowInvalidOperationException_WhenTableNotFoundInDatabase()
    {
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables =
            [
                new TableInfo
                {
                    TableId = 1,
                    Name = "Table1",
                    Columns = [new ColumnInfo { ColumnId = 1, Name = "Column1", TableId = 1 }]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            new() { ColumnId = 2, Name = "Column2", TableId = 2 }  // TableId doesn't exist
        };

        var action = () => SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = action.Should()
            .Throw<InvalidOperationException>();
    }

    [Fact]
    public void GenerateCorrectSql_WithSpecialCharactersInNames()
    {
        const string expectedSql = "USE [Test.Database];\r\nSELECT [Test.Table].[Column.1], [Test.Table].[Column[2]]\r\nFROM [Test.Database].[dbo].[Test.Table] AS [Test.Table]\r\n";
        var databaseInfo = new DatabaseInfo
        {
            Name = "Test.Database",
            Tables =
            [
                new TableInfo
                {
                    TableId = 1,
                    Name = "Test.Table",
                    Columns =
                    [
                        new ColumnInfo { ColumnId = 1, Name = "Column.1", TableId = 1 },
                        new ColumnInfo { ColumnId = 2, Name = "Column[2]", TableId = 1 }
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0],
            databaseInfo.Tables[0].Columns[1]
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleWhitespaceInNames()
    {
        const string expectedSql = "USE [Test Database];\r\nSELECT [Test Table].[Column Name], [Test Table].[Table Column]\r\nFROM [Test Database].[dbo].[Test Table] AS [Test Table]\r\n";
        var databaseInfo = new DatabaseInfo
        {
            Name = "Test Database",
            Tables =
            [
                new TableInfo
                {
                    TableId = 1,
                    Name = "Test Table",
                    Columns =
                    [
                        new ColumnInfo { ColumnId = 1, Name = "Column Name", TableId = 1 },
                        new ColumnInfo { ColumnId = 2, Name = "Table Column", TableId = 1 }
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0],
            databaseInfo.Tables[0].Columns[1]
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo);

        _ = sql.Should().Be(expectedSql);
    }
}
