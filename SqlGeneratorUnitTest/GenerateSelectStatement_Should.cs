namespace SqlGeneratorUnitTest;

using FluentAssertions;
using SqlServerInterrogator.Models;
using SqlServerInterrogator.Services;

public sealed class GenerateSelectStatement_Should
{
    [Fact]
    public void GenerateCorrectSql_WhenAllColumnsToSelectAreInSameTable()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1], [TestTable].[Column2] AS [TestTable.Column2]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
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
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1] AS [Table1.Column1], [Table2].[Column2] AS [Table2.Column2]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key] = [Table2].[ForeignKey]\r\n";
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
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1] AS [Table1.Column1], [Table3].[Column3] AS [Table3.Column3]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key1] = [Table2].[ForeignKey1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table3] AS [Table3]\r\n    ON [Table2].[ForeignKey3] = [Table3].[Key3]\r\n";
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
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1] AS [Table1.Column1], [Table2].[Column2] AS [Table2.Column2], [Table4].[Column4] AS [Table4.Column4]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key1] = [Table2].[ForeignKey1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table3] AS [Table3]\r\n    ON [Table2].[Key2] = [Table3].[ForeignKey2]\r\nLEFT JOIN [TestDatabase].[dbo].[Table4] AS [Table4]\r\n    ON [Table3].[Key3] = [Table4].[ForeignKey3]\r\n";
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
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
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
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1], [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
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
        const string expectedSql = "USE [Test.Database];\r\nSELECT [Test.Table].[Column.1] AS [Test.Table.Column.1], [Test.Table].[Column[2]] AS [Test.Table.Column[2]]\r\nFROM [Test.Database].[dbo].[Test.Table] AS [Test.Table]\r\n";
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
        const string expectedSql = "USE [Test Database];\r\nSELECT [Test Table].[Column Name] AS [Test Table.Column Name], [Test Table].[Table Column] AS [Test Table.Table Column]\r\nFROM [Test Database].[dbo].[Test Table] AS [Test Table]\r\n";
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

    [Fact]
    public void HandleParameters_WhenProvided()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 'SomeValue'\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", "SomeValue")
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithIntegerValue()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 123\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", 123)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithDateTimeValue()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = '2023-10-01 00:00:00'\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", new DateTime(2023, 10, 1))
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithDateTimeValue_WithTimeComponent()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = '2023-10-01 15:30:45'\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", new DateTime(2023, 10, 1, 15, 30, 45))
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithDecimalValue()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 123.45\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", 123.45m)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithStringContainingSingleQuote()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 'O''Reilly'\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", "O'Reilly")
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithEmptyStringValue()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = ''\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", "")
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithUnsupportedType_UsesToString()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = CustomType\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", new { ToString = new Func<string>(() => "CustomType") })
        };
        parameters[0] = ("TestTable", "Column1", new { ToString = new Func<string>(() => "CustomType") });

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithMultipleParameters()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1], [TestTable].[Column2] AS [TestTable.Column2]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 'Value1' AND [TestTable].[Column2] = 42\r\n";
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
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", "Value1"),
            ("TestTable", "Column2", 42)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithNullValue()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] IS NULL\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", null)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithBooleanValue()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 1\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", true)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithCustomObject_WithoutToStringOverride()
    {
        const string expectedSqlStart = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = ";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", new object())
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().StartWith(expectedSqlStart);
        _ = sql.Should().Contain("[TestTable].[Column1] = System.Object");
    }

    [Fact]
    public void HandleParameters_WithDecimalValue_UsesInvariantCulture()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 123.45\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("TestTable", "Column1", 123.45m)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void ReturnEmptyString_WhenAllColumnsAreNull()
    {
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = []
        };

        var columnsToSelect = new List<ColumnInfo?> { null, null };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect!, databaseInfo);
        _ = sql.Should().BeEmpty();
    }

    [Fact]
    public void HandleParameters_WithMixedTypes()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1], [TestTable].[Column2] AS [TestTable.Column2], [TestTable].[Column3] AS [TestTable.Column3], [TestTable].[Column4] AS [TestTable.Column4], [TestTable].[Column5] AS [TestTable.Column5]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\nWHERE [TestTable].[Column1] = 'abc' AND [TestTable].[Column2] = 123 AND [TestTable].[Column3] = 1 AND [TestTable].[Column4] IS NULL AND [TestTable].[Column5] = '2024-01-01 00:00:00'\r\n";
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
                        new ColumnInfo { ColumnId = 2, Name = "Column2", TableId = 1 },
                        new ColumnInfo { ColumnId = 3, Name = "Column3", TableId = 1 },
                        new ColumnInfo { ColumnId = 4, Name = "Column4", TableId = 1 },
                        new ColumnInfo { ColumnId = 5, Name = "Column5", TableId = 1 }
                    ]
                }
            ]
        };
        var columnsToSelect = databaseInfo.Tables[0].Columns;
        var parameters = new List<(string, string, object?)>
        {
            ("TestTable", "Column1", "abc"),
            ("TestTable", "Column2", 123),
            ("TestTable", "Column3", true),
            ("TestTable", "Column4", null),
            ("TestTable", "Column5", new DateTime(2024, 1, 1))
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void NoWhereClause_WhenParametersListIsEmpty()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [TestTable].[Column1] AS [TestTable.Column1]\r\nFROM [TestDatabase].[dbo].[TestTable] AS [TestTable]\r\n";
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
                    ]
                }
            ]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            databaseInfo.Tables[0].Columns[0]
        };
        var parameters = new List<(string, string, object?)>();

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithDirectJoinAndWhereClause()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1] AS [Table1.Column1], [Table2].[Column2] AS [Table2.Column2]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key] = [Table2].[ForeignKey]\r\nWHERE [Table1].[Column1] = 'Value1' AND [Table2].[Column2] = 42\r\n";
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
        table1.TablesICanJoinTo = [table2];
        table2.TablesICanJoinTo = [table1];
        var databaseInfo = new DatabaseInfo
        {
            Name = "TestDatabase",
            Tables = [table1, table2]
        };
        var columnsToSelect = new List<ColumnInfo>
        {
            table1.Columns[0],
            table2.Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("Table1", "Column1", "Value1"),
            ("Table2", "Column2", 42)
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithIndirectJoinAndWhereClause()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1] AS [Table1.Column1], [Table3].[Column3] AS [Table3.Column3]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key1] = [Table2].[ForeignKey1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table3] AS [Table3]\r\n    ON [Table2].[ForeignKey3] = [Table3].[Key3]\r\nWHERE [Table1].[Column1] = 'Value1' AND [Table3].[Column3] = 'Value3'\r\n";
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
            table1.Columns[0],
            table3.Columns[0]
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("Table1", "Column1", "Value1"),
            ("Table3", "Column3", "Value3")
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }

    [Fact]
    public void HandleParameters_WithDirectAndIndirectJoinsAndWhereClause()
    {
        const string expectedSql = "USE [TestDatabase];\r\nSELECT [Table1].[Column1] AS [Table1.Column1], [Table2].[Column2] AS [Table2.Column2], [Table4].[Column4] AS [Table4.Column4]\r\nFROM [TestDatabase].[dbo].[Table1] AS [Table1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table2] AS [Table2]\r\n    ON [Table1].[Key1] = [Table2].[ForeignKey1]\r\nLEFT JOIN [TestDatabase].[dbo].[Table3] AS [Table3]\r\n    ON [Table2].[Key2] = [Table3].[ForeignKey2]\r\nLEFT JOIN [TestDatabase].[dbo].[Table4] AS [Table4]\r\n    ON [Table3].[Key3] = [Table4].[ForeignKey3]\r\nWHERE [Table1].[Column1] = 'A' AND [Table2].[Column2] = 2 AND [Table4].[Column4] = 'Z'\r\n";
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
            table1.Columns[0],  // Table1.Column1
            table2.Columns[0],  // Table2.Column2
            table4.Columns[0]   // Table4.Column4
        };
        var parameters = new List<(string TableName, string ColumnName, object? Value)>
        {
            ("Table1", "Column1", "A"),
            ("Table2", "Column2", 2),
            ("Table4", "Column4", "Z")
        };

        var sql = SqlGenerator.GenerateSelectStatement(columnsToSelect, databaseInfo, parameters);

        _ = sql.Should().Be(expectedSql);
    }
}
