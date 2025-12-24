using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.SetUpVMs;
using System.Data;
using System.Data.SqlClient;

namespace ShampanBFRS.Repository.SetUp
{
    public class ProductRepository : CommonRepository
    {
        // Insert Method       
        public async Task<ResultVM> Insert(ProductVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
INSERT INTO Products
(
Code, Name
,ProductGroupId, ConversionFactor
--, CIFCharge, ExchangeRateUsd, InsuranceRate, BankCharge, OceanLoss, CPACharge, HandelingCharge, LightCharge, Survey, CostLiterExImport
--, ExERLRate, DutyPerLiter, Refined, Crude, SDRate, DutyInTariff, ATRate, VATRate
, IsActive, CreatedBy, CreatedFrom, CreatedAt
)
VALUES
(
@Code, @Name,@ProductGroupId, @ConversionFactor
--, @CIFCharge, @ExchangeRateUsd, @InsuranceRate, @BankCharge, @OceanLoss, @CPACharge, @HandelingCharge, @LightCharge, @Survey
--, @CostLiterExImport, @ExERLRate, @DutyPerLiter, @Refined, @Crude, @SDRate, @DutyInTariff, @ATRate, @VATRate
, @IsActive, @CreatedBy,@CreatedFrom, GETDATE()
);
SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ProductGroupId", vm.ProductGroupId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ConversionFactor", vm.ConversionFactor ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@CreatedBy", vm.CreatedBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@CreatedFrom", vm.CreatedFrom ?? (object)DBNull.Value);

                    //cmd.Parameters.AddWithValue("@CIFCharge", vm.CIFCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@ExchangeRateUsd", vm.ExchangeRateUsd ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@InsuranceRate", vm.InsuranceRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@BankCharge", vm.BankCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@OceanLoss", vm.OceanLoss ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@CPACharge", vm.CPACharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@HandelingCharge", vm.HandelingCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@LightCharge", vm.LightCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@Survey", vm.Survey ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@CostLiterExImport", vm.CostLiterExImport ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@ExERLRate", vm.ExERLRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@DutyPerLiter", vm.DutyPerLiter ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@Refined", vm.Refined ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@Crude", vm.Crude ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@SDRate", vm.SDRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@DutyInTariff", vm.DutyInTariff ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@ATRate", vm.ATRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@VATRate", vm.VATRate ?? (object)DBNull.Value);

                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
                    result.Id = vm.Id.ToString();
                    result.DataVM = vm;
                }


                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Update Method       
        public async Task<ResultVM> Update(ProductVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }

                string query = @"
        UPDATE Products
        SET
                    Code = @Code,
                    Name = @Name,
                    ProductGroupId = @ProductGroupId,
                    ConversionFactor = @ConversionFactor,
                    --//CIFCharge = @CIFCharge,
                    --//ExchangeRateUsd = @ExchangeRateUsd,
                    --//InsuranceRate = @InsuranceRate,
                    --//BankCharge = @BankCharge,
                    --//OceanLoss = @OceanLoss,
                    --//CPACharge = @CPACharge,
                    --//HandelingCharge = @HandelingCharge,
                    --//LightCharge = @LightCharge,
                    --//Survey = @Survey,
                    --//CostLiterExImport = @CostLiterExImport,
                    --//ExERLRate = @ExERLRate,
                    --//DutyPerLiter = @DutyPerLiter,
                    --//Refined = @Refined,
                    --//Crude = @Crude,
                    --//SDRate = @SDRate,
                    --//DutyInTariff = @DutyInTariff,
                    --//ATRate = @ATRate,
                    --//VATRate = @VATRate,
                    IsActive = @IsActive,
                    LastUpdateBy = @LastUpdateBy,
                    LastUpdateFrom = @LastUpdateFrom,
                    LastUpdateAt = GETDATE()
        WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@Code", vm.Code ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@Name", vm.Name ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ProductGroupId", vm.ProductGroupId ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@ConversionFactor", vm.ConversionFactor ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@IsActive", vm.IsActive);
                    cmd.Parameters.AddWithValue("@LastUpdateBy", vm.LastUpdateBy ?? (object)DBNull.Value);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.LastUpdateFrom ?? (object)DBNull.Value);

                    //cmd.Parameters.AddWithValue("@CIFCharge", vm.CIFCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@ExchangeRateUsd", vm.ExchangeRateUsd ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@InsuranceRate", vm.InsuranceRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@BankCharge", vm.BankCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@OceanLoss", vm.OceanLoss ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@CPACharge", vm.CPACharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@HandelingCharge", vm.HandelingCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@LightCharge", vm.LightCharge ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@Survey", vm.Survey ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@CostLiterExImport", vm.CostLiterExImport ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@ExERLRate", vm.ExERLRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@DutyPerLiter", vm.DutyPerLiter ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@Refined", vm.Refined ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@Crude", vm.Crude ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@SDRate", vm.SDRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@DutyInTariff", vm.DutyInTariff ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@ATRate", vm.ATRate ?? (object)DBNull.Value);
                    //cmd.Parameters.AddWithValue("@VATRate", vm.VATRate ?? (object)DBNull.Value);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.UpdateSuccess;
                    }
                    else
                    {
                        throw new Exception(result.Message);
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // Delete Method       
        public async Task<ResultVM> Delete(CommonVM vm, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                if (transaction == null)
                {
                    transaction = conn.BeginTransaction();
                }


                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE Products SET IsArchive = 1, IsActive = 0,LastModifiedBy = @LastModifiedBy,LastUpdateFrom = @LastUpdateFrom ,LastModifiedOn =GETDATE() WHERE Id IN ({inClause})";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@LastModifiedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@LastUpdateFrom", vm.ModifyFrom);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.DeleteSuccess;
                    }
                    else
                    {
                        throw new Exception("No rows were deleted.");
                    }
                }


                return result;
            }
            catch (Exception ex)
            {

                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"
                                SELECT
                                    ISNULL(Id, 0)                AS Id,
                                    ISNULL(Code, '')             AS Code,
                                    ISNULL(Name, '')             AS Name,
                                    ISNULL(ProductGroupId, 0)    AS ProductGroupId,
                                    ISNULL(ConversionFactor, 0)  AS ConversionFactor,
                                    ISNULL(IsActive, 0)          AS IsActive,
                                    ISNULL(CreatedBy, '')        AS CreatedBy,
                                    ISNULL(CONVERT(VARCHAR(19), CreatedAt, 120), '')    AS CreatedAt,
                                    ISNULL(CreatedFrom, '')      AS CreatedFrom,
                                    ISNULL(LastUpdateBy, '')     AS LastUpdateBy,
                                    ISNULL(CONVERT(VARCHAR(19), LastUpdateAt, 120), '')    AS LastUpdateAt,
                                    ISNULL(LastUpdateFrom, '')   AS LastUpdateFrom
                                FROM Products Where 1=1
                                 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new ProductVM
                {
                    Id = row.Field<int>("Id"),
                    Code = row.Field<string>("Code"),
                    Name = row.Field<string>("Name"),
                    ProductGroupId = row.Field<int>("ProductGroupId"),
                    ConversionFactor = row.Field<decimal>("ConversionFactor"),
                    IsActive = row.Field<bool>("IsActive"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    CreatedAt = row.Field<string>("CreatedAt"),
                    CreatedFrom = row.Field<string>("CreatedFrom"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateAt = row.Field<string>("LastUpdateAt"),
                    LastUpdateFrom = row.Field<string>("LastUpdateFrom")
                }).ToList();


                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = list;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }

        public async Task<ResultVM> EstimatedList(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null,
            SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error" };

            try
            {
                if (conn == null) throw new Exception(MessageModel.DBConnFail);
                if (transaction == null) throw new Exception(MessageModel.DBConnFail);

                string query = @"
--                SELECT
--                    ISNULL(M.Id, 0) AS Id,
--             ISNULL(M.Code, '') AS Code,
--             ISNULL(M.Name, '') AS Name,
--             ISNULL(M.ConversionFactor, 0) AS ConversionFactor,
--             --ISNULL(M.CIFCharge, 0) AS CIFCharge,
--             --ISNULL(M.ExchangeRateUsd, 0) AS ExchangeRateUsd,
--             --ISNULL(M.InsuranceRate, 0) AS InsuranceRate,
--             --ISNULL(M.BankCharge, 0) AS BankCharge,
--             --ISNULL(M.OceanLoss, 0) AS OceanLoss,
--             --ISNULL(M.CPACharge, 0) AS CPACharge,
--             --ISNULL(M.HandelingCharge, 0) AS HandelingCharge,
--             --ISNULL(M.LightCharge, 0) AS LightCharge,
--             --ISNULL(M.Survey, 0) AS Survey,
--             --ISNULL(M.CostLiterExImport, 0) AS CostLiterExImport,
--             --ISNULL(M.ExERLRate, 0) AS ExERLRate,
--             --ISNULL(M.DutyPerLiter, 0) AS DutyPerLiter,
--             --ISNULL(M.Refined, 0) AS Refined,
--             --ISNULL(M.Crude, 0) AS Crude,
--             --ISNULL(M.SDRate, 0) AS SDRate,
--             --ISNULL(M.DutyInTariff, 0) AS DutyInTariff,
--             --ISNULL(M.ATRate, 0) AS ATRate,
--             --ISNULL(M.VATRate, 0) AS VATRate,
--             ISNULL(M.IsActive, 0) AS IsActive,
--            ISNULL(M.CreatedBy, '') AS CreatedBy,
--             ISNULL(FORMAT(M.CreatedAt, 'yyyy-MM-dd HH:mm'), '') AS CreatedAt,
--             ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
--             ISNULL(FORMAT(M.LastUpdateAt, 'yyyy-MM-dd HH:mm'), '') AS LastUpdateAt
--         FROM Products M
--WHERE 1 = 1



select 
             ISNULL(p.Id, 0) AS Id,
             ISNULL(p.Code, '') AS Code,
             ISNULL(p.Name, '') AS Name,
             ISNULL(p.ConversionFactor, 0) AS ConversionFactor,
			 ISNULL(ch.ChargeGroup, 0) AS ChargeGroup,
			 ISNULL(p.ProductGroupId, 0) AS ProductGroupId,
			 ISNULL(pg.Name, 0) AS ProductGroupName,
			 ISNULL(cd.CIFCharge, 0) AS CIFCharge,
             ISNULL(cd.ExchangeRateUsd, 0) AS ExchangeRateUsd,
             ISNULL(cd.InsuranceRate, 0) AS InsuranceRate,
             ISNULL(cd.BankCharge, 0) AS BankCharge,
             ISNULL(cd.OceanLoss, 0) AS OceanLoss,
             ISNULL(cd.CPACharge, 0) AS CPACharge,
             ISNULL(cd.HandelingCharge, 0) AS HandelingCharge,
             ISNULL(cd.LightCharge, 0) AS LightCharge,
             ISNULL(cd.Survey, 0) AS Survey,
             ISNULL(cd.CostLiterExImport, 0) AS CostLiterExImport,
             ISNULL(cd.ExERLRate, 0) AS ExERLRate,
             ISNULL(cd.DutyPerLiter, 0) AS DutyPerLiter,
             ISNULL(cd.Refined, 0) AS Refined,
             ISNULL(cd.Crude, 0) AS Crude,
             ISNULL(cd.SDRate, 0) AS SDRate,
             ISNULL(cd.DutyInTariff, 0) AS DutyInTariff,
             ISNULL(cd.ATRate, 0) AS ATRate,
             ISNULL(cd.VATRate, 0) AS VATRate,
			 ISNULL(cd.AITRate, 0) AS AITRate,
			 ISNULL(cd.ConversionFactorFixedValue, 0) AS ConversionFactorFixedValue,
			 ISNULL(cd.VATRateFixed, 0) AS VATRateFixed,
			 ISNULL(cd.RiverDues, 0) AS RiverDues,
			 ISNULL(cd.TariffRate, 0) AS TariffRate,
			 ISNULL(cd.FobPriceBBL, 0) AS FobPriceBBL,
			 ISNULL(cd.FreightUsd, 0) AS FreightUsd,
			 ISNULL(cd.ServiceCharge, 0) AS ServiceCharge,
			 ISNULL(cd.ProcessFee, 0) AS ProcessFee,
			 ISNULL(cd.RcoTreatmentFee, 0) AS RcoTreatmentFee,
			 ISNULL(cd.AbpTreatmentFee, 0) AS AbpTreatmentFee,
			 ISNULL(p.IsActive, 0) AS IsActive,
             ISNULL(p.CreatedBy, '') AS CreatedBy,
             ISNULL(FORMAT(p.CreatedAt, 'yyyy-MM-dd HH:mm'), '') AS CreatedAt,
             ISNULL(p.LastUpdateBy, '') AS LastUpdateBy,
             ISNULL(FORMAT(p.LastUpdateAt, 'yyyy-MM-dd HH:mm'), '') AS LastUpdateAt

			 from ChargeHeaders ch
			left outer join ChargeDetails cd on cd.ChargeHeaderId = ch.Id
			left outer join Products p on cd.ProductId = p.Id
			left outer join ProductGroups pg on pg.Id = p.ProductGroupId
			Where 1=1
 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    query += " AND p.Id=@Id ";

                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter adapter = CreateAdapter(query, conn, transaction);
                adapter.SelectCommand = ApplyParameters(adapter.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                    adapter.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);

                adapter.Fill(dt);

                var list = dt.AsEnumerable().Select(row => new ProductVM
                {
                    Id = row.Field<int>("Id"),
                    Code = row.Field<string>("Code"),
                    Name = row.Field<string>("Name"),
                    ProductGroupName = row.Field<string>("ProductGroupName"),
                    ProductGroupId = row.Field<int>("ProductGroupId"),
                    ConversionFactor = row.Field<decimal>("ConversionFactor"),
                    CIFCharge = row.Field<decimal>("CIFCharge"),
                    ExchangeRateUsd = row.Field<decimal>("ExchangeRateUsd"),
                    InsuranceRate = row.Field<decimal>("InsuranceRate"),
                    BankCharge = row.Field<decimal>("BankCharge"),
                    OceanLoss = row.Field<decimal>("OceanLoss"),
                    CPACharge = row.Field<decimal>("CPACharge"),
                    HandelingCharge = row.Field<decimal>("HandelingCharge"),
                    LightCharge = row.Field<decimal>("LightCharge"),
                    Survey = row.Field<decimal>("Survey"),
                    CostLiterExImport = row.Field<decimal>("CostLiterExImport"),
                    ExERLRate = row.Field<decimal>("ExERLRate"),
                    DutyPerLiter = row.Field<decimal>("DutyPerLiter"),
                    Refined = row.Field<decimal>("Refined"),
                    Crude = row.Field<decimal>("Crude"),
                    SDRate = row.Field<decimal>("SDRate"),
                    DutyInTariff = row.Field<decimal>("DutyInTariff"),
                    ATRate = row.Field<decimal>("ATRate"),
                    VATRate = row.Field<decimal>("VATRate"),
                    AITRate = row.Field<decimal>("AITRate"),
                    ConversionFactorFixedValue = row.Field<decimal>("ConversionFactorFixedValue"),
                    VATRateFixed = row.Field<decimal>("VATRateFixed"),
                    RiverDues = row.Field<decimal>("RiverDues"),
                    TariffRate = row.Field<decimal>("TariffRate"),
                    FobPriceBBL = row.Field<decimal>("FobPriceBBL"),
                    FreightUsd = row.Field<decimal>("FreightUsd"),
                    ServiceCharge = row.Field<decimal>("ServiceCharge"),
                    ProcessFee = row.Field<decimal>("ProcessFee"),
                    RcoTreatmentFee = row.Field<decimal>("RcoTreatmentFee"),
                    AbpTreatmentFee = row.Field<decimal>("AbpTreatmentFee"),
                    IsActive = row.Field<bool>("IsActive"),
                    CreatedBy = row.Field<string>("CreatedBy"),
                    LastUpdateBy = row.Field<string>("LastUpdateBy"),
                    LastUpdateAt = row.Field<string>("LastUpdateAt")
                }).ToList();


                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = list;

                return result;
            }
            catch (Exception ex)
            {
                result.Status = MessageModel.Fail;
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
                return result;
            }
        }
        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
SELECT
    Id, Code, Name, Description,
    IsArchive, IsActive, CreatedBy, CreatedOn, 
    LastModifiedBy, LastModifiedOn
FROM Products 
WHERE 1 = 1 ";



                DataTable dataTable = new DataTable();

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND Id = @Id ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValues, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValues);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        // Dropdown Method
        public async Task<ResultVM> Dropdown(SqlConnection conn = null, SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
SELECT Id, Name
FROM Products
WHERE IsActive = 1
ORDER BY Name";

                DataTable dropdownData = new DataTable();
                using (SqlDataAdapter adapter = new SqlDataAdapter(query, conn))
                {
                    if (transaction != null)
                    {
                        adapter.SelectCommand.Transaction = transaction;
                    }
                    adapter.Fill(dropdownData);
                }

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dropdownData;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        // GetGridData Method
        public async Task<ResultVM> GetGridData(GridOptions options, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<ProductVM>();

                // Define your SQL query string
                string sqlQuery = @"
            -- Count query
            SELECT COUNT(DISTINCT M.Id) AS totalcount
            FROM Products M
            WHERE 1= 1
            -- Add the filter condition
            " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ProductVM>.FilterCondition(options.filter) + ")" : "") + @"

            -- Data query with pagination and sorting
            SELECT * 
            FROM (
                SELECT 
                ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "M.Id DESC ") + @") AS rowindex,
             ISNULL(M.Id, 0) AS Id,
             ISNULL(M.Code, '') AS Code,
             ISNULL(M.Name, '') AS Name,
             ISNULL(M.ConversionFactor, 0) AS ConversionFactor,
             --ISNULL(M.CIFCharge, 0) AS CIFCharge,
             --ISNULL(M.ExchangeRateUsd, 0) AS ExchangeRateUsd,
             --ISNULL(M.InsuranceRate, 0) AS InsuranceRate,
             --ISNULL(M.BankCharge, 0) AS BankCharge,
             --ISNULL(M.OceanLoss, 0) AS OceanLoss,
             --ISNULL(M.CPACharge, 0) AS CPACharge,
             --ISNULL(M.HandelingCharge, 0) AS HandelingCharge,
             --ISNULL(M.LightCharge, 0) AS LightCharge,
             --ISNULL(M.Survey, 0) AS Survey,
             --ISNULL(M.CostLiterExImport, 0) AS CostLiterExImport,
             --ISNULL(M.ExERLRate, 0) AS ExERLRate,
             --ISNULL(M.DutyPerLiter, 0) AS DutyPerLiter,
             --ISNULL(M.Refined, 0) AS Refined,
             --ISNULL(M.Crude, 0) AS Crude,
             --ISNULL(M.SDRate, 0) AS SDRate,
             --ISNULL(M.DutyInTariff, 0) AS DutyInTariff,
             --ISNULL(M.ATRate, 0) AS ATRate,
             --ISNULL(M.VATRate, 0) AS VATRate,
             ISNULL(M.IsActive, 0) AS IsActive,
             ISNULL(M.CreatedBy, '') AS CreatedBy,
             ISNULL(FORMAT(M.CreatedAt, 'yyyy-MM-dd HH:mm'), '') AS CreatedAt,
             ISNULL(M.LastUpdateBy, '') AS LastUpdateBy,
             ISNULL(FORMAT(M.LastUpdateAt, 'yyyy-MM-dd HH:mm'), '') AS LastUpdateAt
         FROM Products M
            WHERE 1= 1
            -- Add the filter condition
            " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ProductVM>.FilterCondition(options.filter) + ")" : "") + @"

            ) AS a
            WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
        ";

                data = KendoGrid<ProductVM>.GetGridData_CMD(options, sqlQuery, "M.Id");

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = data;

                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }

        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                string query = @"
                SELECT 

                ISNULL(H.Id, 0) AS Id,
                ISNULL(H.Code, '') AS Code,
                ISNULL(H.Name, '') AS Name,
                ISNULL(H.Description, '') AS Description,
                ISNULL(H.IsArchive, 0) AS IsArchive,                
                CASE WHEN ISNULL(H.IsActive, 0) = 1 THEN 'Active' ELSE 'Inactive' END AS Status,
                ISNULL(H.CreatedBy, '') AS CreatedBy,
                ISNULL(H.LastModifiedBy, '') AS LastModifiedBy,
                ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') AS CreatedOn,
                ISNULL(FORMAT(H.LastModifiedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') AS LastModifiedOn

            FROM Products H

            WHERE  1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND H.Id = @Id ";
                }

                // Apply additional conditions
                query = ApplyConditions(query, conditionalFields, conditionalValue, false);

                SqlDataAdapter objComm = CreateAdapter(query, conn, transaction);

                // SET additional conditions param
                objComm.SelectCommand = ApplyParameters(objComm.SelectCommand, conditionalFields, conditionalValue);

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    objComm.SelectCommand.Parameters.AddWithValue("@Id", vm.Id);
                }

                objComm.Fill(dataTable);

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = dataTable;
                return result;
            }
            catch (Exception ex)
            {
                result.ExMessage = ex.Message;
                result.Message = ex.Message;
                return result;
            }
        }





    }

}
