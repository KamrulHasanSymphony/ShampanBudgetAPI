using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.Repository.Common;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using ShampanBFRS.ViewModel.Utility;
using ShampanBFRS.ViewModel.KendoCommon;
using ShampanBFRS.ViewModel.QuestionVM;
using Newtonsoft.Json;

namespace ShampanBFRS.Repository.SetUp
{
    public class ChargeHeaderRepository : CommonRepository
    {
        // Insert Method
        public async Task<ResultVM> Insert(ChargeHeaderVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                string query = @"
                INSERT INTO ChargeHeaders
                (
                 ChargeGroup
                )
                VALUES
                (
                    @ChargeGroup
                );
                SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@ChargeGroup", vm.ChargeGroup ?? (object)DBNull.Value);
                    vm.Id = Convert.ToInt32(cmd.ExecuteScalar());

                    result.Status =MessageModel.Success;
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
        public async Task<ResultVM> Update(ChargeHeaderVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.Id.ToString(), DataVM = vm };

            try
            {
                string query = @"
                UPDATE ChargeHeaders
                SET
                    ChargeGroup = @ChargeGroup
                WHERE Id = @Id";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@Id", vm.Id);
                    cmd.Parameters.AddWithValue("@ChargeGroup", vm.ChargeGroup ?? (object)DBNull.Value);
            
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
        public async Task<ResultVM> MultipleDelete(CommonVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE ChargeHeaders SET IsArchive = 1, IsActive = 0, LastModifiedBy = @LastModifiedBy, LastUpdateFrom = @LastUpdateFrom, LastModifiedOn = GETDATE() WHERE Id IN ({inClause})";

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

        // List Method
        public async Task<ResultVM> List(string[] conditionalFields, string[] conditionalValues , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, DataVM = null };

            try
            {
                string query = @"
                SELECT 
                    ISNULL(M.Id, 0) AS Id,
                    ISNULL(M.ChargeGroup, '') AS ChargeGroup
  
                FROM ChargeHeaders M
                WHERE 1 = 1
                ";

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

                var modelList = dataTable.AsEnumerable().Select(row => new ChargeHeaderVM
                {
                    Id = row.Field<int>("Id"),
                    ChargeGroup = row.Field<string>("ChargeGroup")         
                }).ToList();

                result.Status = MessageModel.Success;
                result.Message = MessageModel.RetrievedSuccess;
                result.DataVM = modelList;
                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }

        // ListAsDataTable Method
        public async Task<ResultVM> ListAsDataTable(string[] conditionalFields, string[] conditionalValues
           , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            try
            {
                string query = @"
                SELECT 
                    Id, ChargeGroup
                FROM ChargeHeaders
                WHERE 1 = 1
                ";

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
                result.Message =MessageModel.RetrievedSuccess;
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
        public async Task<ResultVM> Dropdown(SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null };

            try
            {
                string query = @"
                SELECT Id, ChargeGroup
                FROM ChargeHeaders
                WHERE IsActive = 1
                ORDER BY Code";

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
        public async Task<ResultVM> GetGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };


            try
            {
                if (conn == null)
                {
                    throw new Exception("Database connection fail!");
                }

                var data = new GridEntity<ChargeHeaderVM>();

                // Define your SQL query string
                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT H.Id) AS totalcount
                  FROM ChargeHeaders H
                LEFT OUTER JOIN ChargeGroups CG ON CG.Id = H.ChargeGroup
            WHERE 1 = 1
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ChargeHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                -- Data query with pagination and sorting
                SELECT * 
                FROM (
                    SELECT 
                    ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "H.Id DESC") + @") AS rowindex,

                   
    
                 ISNULL(H.Id, 0) AS Id,
                 ISNULL(CG.ChargeGroupText, '') AS ChargeGroup
  
                FROM ChargeHeaders H
                LEFT OUTER JOIN ChargeGroups CG ON CG.Id = H.ChargeGroup
                WHERE 1 = 1

                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ChargeHeaderVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
            ";

                // Execute the query and get data
                data = KendoGrid<ChargeHeaderVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

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
        // ReportPreview Method
        public async Task<ResultVM> ReportPreview(string[] conditionalFields, string[] conditionalValue
            , SqlConnection conn, SqlTransaction transaction, PeramModel vm = null)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                string query = @"
                SELECT 
                    ISNULL(H.Id, 0) AS Id,
                    ISNULL(H.Code, '') AS Code,
                    ISNULL(H.TransactionDate, '') AS TransactionDate,
                    ISNULL(H.TransactionType, '') AS TransactionType,
                    ISNULL(H.SupplierId, 0) AS SupplierId,
                    ISNULL(H.Status, '') AS Status,
                    ISNULL(H.Notes, '') AS Notes,
                    ISNULL(H.CreatedBy, '') AS CreatedBy,
                    ISNULL(FORMAT(H.CreatedOn, 'yyyy-MM-dd HH:mm'), '1900-01-01') AS CreatedOn
                FROM FM_FeedPurchaseHeaders H
                WHERE 1 = 1 ";

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
                result.Message =MessageModel.RetrievedSuccess;
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

        // MultiplePost Method
        public async Task<ResultVM> MultiplePost(CommonVM vm, SqlConnection conn, SqlTransaction transaction)
        {
            ResultVM result = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = vm.IDs.ToString(), DataVM = null };

            try
            {
                string inClause = string.Join(", ", vm.IDs.Select((id, index) => $"@Id{index}"));

                string query = $" UPDATE ChargeHeaders SET IsPost = 1, PostedBy = @PostedBy , PostedFrom = @PostedFrom ,PostedOn = GETDATE() WHERE Id IN ({inClause}) ";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    for (int i = 0; i < vm.IDs.Length; i++)
                    {
                        cmd.Parameters.AddWithValue($"@Id{i}", vm.IDs[i]);
                    }

                    cmd.Parameters.AddWithValue("@PostedBy", vm.ModifyBy);
                    cmd.Parameters.AddWithValue("@PostedFrom", vm.ModifyFrom);

                    int rowsAffected = cmd.ExecuteNonQuery();
                    if (rowsAffected > 0)
                    {
                        result.Status = MessageModel.Success;
                        result.Message = MessageModel.RetrievedSuccess;
                    }
                    else
                    {
                        throw new Exception("No rows were posted.");
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Message = ex.Message;
                result.ExMessage = ex.Message;
                return result;
            }
        }
        public async Task<ResultVM> InsertDetails(ChargeDetailVM details, SqlConnection conn = null,  SqlTransaction transaction = null)
        {
            ResultVM result = new ResultVM
            {
                Status = MessageModel.Fail,
                Message = "Error",
                ExMessage = null,
                Id = "0",
                DataVM = null
            };

            try
            {
                string query = @"
        INSERT INTO ChargeDetails
        (
            ChargeHeaderId,
            ProductId,
            CIFCharge,
            ExchangeRateUsd,
            InsuranceRate,
            BankCharge,
            OceanLoss,
            CPACharge,
            HandelingCharge,
            LightCharge,
            Survey,
            CostLiterExImport,
            ExERLRate,
            DutyPerLiter,
            Refined,
            Crude,
            SDRate,
            DutyInTariff,
            ATRate,
            AITRate,
            VATRate,
            ConversionFactorFixedValue,
            VATRateFixed,
            RiverDues,

            TariffRate,
            FobPriceBBL,
            FreightUsd,
            ServiceCharge,
            ProcessFee,
            RcoTreatmentFee,
            AbpTreatmentFee,

            ProcessFeeRate,
            RcoTreatmentFeeRate,
            AbpTreatmentFeeRate,
            ProductImprovementFee

        )
        VALUES
        (
            @ChargeHeaderId,
            @ProductId,
            @CIFCharge,
            @ExchangeRateUsd,
            @InsuranceRate,
            @BankCharge,
            @OceanLoss,
            @CPACharge,
            @HandelingCharge,
            @LightCharge,
            @Survey,
            @CostLiterExImport,
            @ExERLRate,
            @DutyPerLiter,
            @Refined,
            @Crude,
            @SDRate,
            @DutyInTariff,
            @ATRate,
            @AITRate,
            @VATRate,
            @ConversionFactorFixedValue,
            @VATRateFixed,
            @RiverDues,

            @TariffRate,
            @FobPriceBBL,
            @FreightUsd,
            @ServiceCharge,
            @ProcessFee,
            @RcoTreatmentFee,
            @AbpTreatmentFee,

            @ProcessFeeRate,
            @RcoTreatmentFeeRate,
            @AbpTreatmentFeeRate,
            @ProductImprovementFee
               
        );

        SELECT SCOPE_IDENTITY();";

                using (SqlCommand cmd = new SqlCommand(query, conn, transaction))
                {
                    cmd.Parameters.AddWithValue("@ChargeHeaderId", details.ChargeHeaderId);
                    cmd.Parameters.AddWithValue("@ProductId", details.ProductId);

                    cmd.Parameters.AddWithValue("@CIFCharge", details.CIFCharge);
                    cmd.Parameters.AddWithValue("@ExchangeRateUsd", details.ExchangeRateUsd);
                    cmd.Parameters.AddWithValue("@InsuranceRate", details.InsuranceRate);
                    cmd.Parameters.AddWithValue("@BankCharge", details.BankCharge);
                    cmd.Parameters.AddWithValue("@OceanLoss", details.OceanLoss);
                    cmd.Parameters.AddWithValue("@CPACharge", details.CPACharge);
                    cmd.Parameters.AddWithValue("@HandelingCharge", details.HandelingCharge);
                    cmd.Parameters.AddWithValue("@LightCharge", details.LightCharge);
                    cmd.Parameters.AddWithValue("@Survey", details.Survey);
                    cmd.Parameters.AddWithValue("@CostLiterExImport", details.CostLiterExImport);
                    cmd.Parameters.AddWithValue("@ExERLRate", details.ExERLRate);
                    cmd.Parameters.AddWithValue("@DutyPerLiter", details.DutyPerLiter);
                    cmd.Parameters.AddWithValue("@Refined", details.Refined);
                    cmd.Parameters.AddWithValue("@Crude", details.Crude);
                    cmd.Parameters.AddWithValue("@SDRate", details.SDRate);
                    cmd.Parameters.AddWithValue("@DutyInTariff", details.DutyInTariff);
                    cmd.Parameters.AddWithValue("@ATRate", details.ATRate);
                    cmd.Parameters.AddWithValue("@AITRate", details.AITRate);
                    cmd.Parameters.AddWithValue("@VATRate", details.VATRate);
                    cmd.Parameters.AddWithValue("@ConversionFactorFixedValue", details.ConversionFactorFixedValue);
                    cmd.Parameters.AddWithValue("@VATRateFixed", details.VATRateFixed);
                    cmd.Parameters.AddWithValue("@RiverDues", details.RiverDues);

                    cmd.Parameters.AddWithValue("@TariffRate", details.TariffRate);
                    cmd.Parameters.AddWithValue("@FobPriceBBL", details.FobPriceBBL);
                    cmd.Parameters.AddWithValue("@FreightUsd", details.FreightUsd);
                    cmd.Parameters.AddWithValue("@ServiceCharge", details.ServiceCharge);
                    cmd.Parameters.AddWithValue("@ProcessFee", details.ProcessFee);
                    cmd.Parameters.AddWithValue("@RcoTreatmentFee", details.RcoTreatmentFee);
                    cmd.Parameters.AddWithValue("@AbpTreatmentFee", details.AbpTreatmentFee);

                    cmd.Parameters.AddWithValue("@ProcessFeeRate", details.ProcessFeeRate);
                    cmd.Parameters.AddWithValue("@RcoTreatmentFeeRate", details.RcoTreatmentFeeRate);
                    cmd.Parameters.AddWithValue("@AbpTreatmentFeeRate", details.AbpTreatmentFeeRate);
                    cmd.Parameters.AddWithValue("@ProductImprovementFee", details.ProductImprovementFee);


                    object newId = await cmd.ExecuteScalarAsync();
                    details.Id = Convert.ToInt32(newId);

                    result.Status = MessageModel.Success;
                    result.Message = MessageModel.InsertSuccess;
                    result.Id = newId.ToString();
                    result.DataVM = details;
                }
            }
            catch (Exception ex)
            {
                result.Status = "Fail";
                result.Message = ex.Message;
                result.ExMessage = ex.ToString();
            }

            return result;
        }


        //DetailsList
        public async Task<ResultVM> DetailsList(string[] conditionalFields, string[] conditionalValue, PeramModel vm = null, SqlConnection conn = null, SqlTransaction transaction = null)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                if (conn == null)
                {
                    conn = new SqlConnection(DatabaseHelper.GetConnectionString());
                    conn.Open();
                    isNewConnection = true;
                }

                string query = @"
             SELECT 
             ISNULL(D.Id, 0) AS Id
            ,D.ChargeHeaderId
            ,D.ProductId
            ,P.Name AS ProductName
            ,D.CIFCharge
            ,D.ExchangeRateUsd
            ,D.InsuranceRate
            ,D.BankCharge
            ,D.OceanLoss
            ,D.CPACharge
            ,D.HandelingCharge
            ,D.LightCharge
            ,D.Survey
            ,D.CostLiterExImport
            ,D.ExERLRate
            ,D.DutyPerLiter
            ,D.Refined
            ,D.Crude
            ,D.SDRate
            ,D.DutyInTariff
            ,D.ATRate
            ,D.AITRate
            ,D.VATRate
            ,D.ConversionFactorFixedValue
            ,D.VATRateFixed
            ,D.RiverDues

            ,D.TariffRate
            ,D.FobPriceBBL
            ,D.FreightUsd
            ,D.ServiceCharge
            ,D.ProcessFee
            ,D.RcoTreatmentFee
            ,D.AbpTreatmentFee

            ,D.ProcessFeeRate
            ,D.RcoTreatmentFeeRate
            ,D.AbpTreatmentFeeRate
            ,D.ProductImprovementFee

            

        FROM ChargeDetails D
        LEFT JOIN Products P ON D.ProductId = P.Id
  
                   WHERE 1 = 1 ";

                if (vm != null && !string.IsNullOrEmpty(vm.Id))
                {
                    query += " AND D.ChargeHeaderId = @Id ";
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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }
        //GetPurchaseDetailDataById
        public async Task<ResultVM> GetChargeDetailDataById(GridOptions options, int masterId, SqlConnection conn, SqlTransaction transaction)
        {
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {
                var data = new GridEntity<ChargeDetailVM>();

                string sqlQuery = @"
                -- Count query
                SELECT COUNT(DISTINCT D.Id) AS totalcount
        FROM ChargeDetails D
     
        WHERE D.ChargeHeaderId = @masterId
                -- Add the filter condition
                " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ChargeDetailVM>.FilterCondition(options.filter) + ")" : "") + @"

                -- Data query with pagination and sorting
                SELECT *
                FROM (
                    SELECT
                        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "D.Id DESC ") + @") AS rowindex,
                                ISNULL(D.Id, 0) AS Id,
                                ISNULL(D.ChargeHeaderId, 0) AS ChargeHeaderId,
                                ISNULL(D.ProductId, 0) AS ProductId,
                                ISNULL(P.Name, 0) AS ProductName,
                                ISNULL(D.CIFCharge, 0) AS CIFCharge,
                                ISNULL(D.ExchangeRateUsd, 0) AS ExchangeRateUsd,
                                ISNULL(D.InsuranceRate, 0) AS InsuranceRate,
                                ISNULL(D.BankCharge, 0) AS BankCharge,
                                ISNULL(D.OceanLoss, 0) AS OceanLoss,
                                ISNULL(D.CPACharge, 0) AS CPACharge,
                                ISNULL(D.HandelingCharge, 0) AS HandelingCharge,
                                ISNULL(D.LightCharge, 0) AS LightCharge,
                                ISNULL(D.Survey, 0) AS Survey,
                                ISNULL(D.CostLiterExImport, 0) AS CostLiterExImport,
                                ISNULL(D.ExERLRate, 0) AS ExERLRate,
                                ISNULL(D.DutyPerLiter, 0) AS DutyPerLiter,
                                ISNULL(D.Refined, 0) AS Refined,
                                ISNULL(D.Crude, 0) AS Crude,
                                ISNULL(D.SDRate, 0) AS SDRate,
                                ISNULL(D.DutyInTariff, 0) AS DutyInTariff,
                                ISNULL(D.ATRate, 0) AS ATRate,
                                ISNULL(D.AITRate, 0) AS AITRate,
                                ISNULL(D.VATRate, 0) AS VATRate,
                                ISNULL(D.ConversionFactorFixedValue, 0) AS ConversionFactorFixedValue,
                                ISNULL(D.VATRateFixed, 0) AS VATRateFixed,
                                ISNULL(D.RiverDues, 0) AS RiverDues,

                                ISNULL(D.TariffRate, 0) AS TariffRate,
                                ISNULL(D.FobPriceBBL, 0) AS FobPriceBBL,
                                ISNULL(D.FreightUsd, 0) AS FreightUsd,
                                ISNULL(D.ServiceCharge, 0) AS ServiceCharge,
                                ISNULL(D.ProcessFee, 0) AS ProcessFee,
                                ISNULL(D.RcoTreatmentFee, 0) AS RcoTreatmentFee,
                                ISNULL(D.AbpTreatmentFee, 0) AS AbpTreatmentFee,

                                ISNULL(D.ProcessFeeRate, 0) AS ProcessFeeRate,
                                ISNULL(D.RcoTreatmentFeeRate, 0) AS RcoTreatmentFeeRate,
                                ISNULL(D.AbpTreatmentFeeRate, 0) AS AbpTreatmentFeeRate,
                                ISNULL(D.ProductImprovementFee, 0) AS ProductImprovementFee

                                FROM ChargeDetails D
                                LEFT OUTER JOIN Products P ON D.ProductId =P.Id
                                Where D.ChargeHeaderId = @masterId


                    -- Add the filter condition
                    " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ChargeDetailVM>.FilterCondition(options.filter) + ")" : "") + @"
                ) AS a
                WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
                ";
                sqlQuery = sqlQuery.Replace("@masterId", "" + masterId + "");
                data = KendoGrid<ChargeDetailVM>.GetGridData_CMD(options, sqlQuery, "H.Id");

                result.Status = MessageModel.Success;
                result.Message =MessageModel.RetrievedSuccess;
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


        //GetDetailsGridData
        public async Task<ResultVM> GetDetailsGridData(GridOptions options, string[] conditionalFields, string[] conditionalValues, SqlConnection conn, SqlTransaction transaction)
        {
            bool isNewConnection = false;
            DataTable dataTable = new DataTable();
            ResultVM result = new ResultVM { Status = MessageModel.Fail, Message = "Error", ExMessage = null, Id = "0", DataVM = null };

            try
            {

                var data = new GridEntity<ChargeDetailVM>();

                // Define your SQL query string
                string sqlQuery = $@"
    -- Count query
    SELECT COUNT(DISTINCT H.Id) AS totalcount
            FROM ChargeDetails  H
     

	WHERE 1 = 1
    -- Add the filter condition
        " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ChargeDetailVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    -- Data query with pagination and sorting
    SELECT * 
    FROM (
        SELECT 
        ROW_NUMBER() OVER(ORDER BY " + (options.sort.Count > 0 ? options.sort[0].field + " " + options.sort[0].dir : "H.Id DESC") + $@") AS rowindex,
        
                                ISNULL(D.Id, 0) AS Id,
                                ISNULL(D.ChargeHeaderId, 0) AS ChargeHeaderId,
                                ISNULL(D.ProductId, 0) AS ProductId,
                                ISNULL(P.Name, 0) AS ProductName,
                                ISNULL(D.CIFCharge, 0) AS CIFCharge,
                                ISNULL(D.ExchangeRateUsd, 0) AS ExchangeRateUsd,
                                ISNULL(D.InsuranceRate, 0) AS InsuranceRate,
                                ISNULL(D.BankCharge, 0) AS BankCharge,
                                ISNULL(D.OceanLoss, 0) AS OceanLoss,
                                ISNULL(D.CPACharge, 0) AS CPACharge,
                                ISNULL(D.HandelingCharge, 0) AS HandelingCharge,
                                ISNULL(D.LightCharge, 0) AS LightCharge,
                                ISNULL(D.Survey, 0) AS Survey,
                                ISNULL(D.CostLiterExImport, 0) AS CostLiterExImport,
                                ISNULL(D.ExERLRate, 0) AS ExERLRate,
                                ISNULL(D.DutyPerLiter, 0) AS DutyPerLiter,
                                ISNULL(D.Refined, 0) AS Refined,
                                ISNULL(D.Crude, 0) AS Crude,
                                ISNULL(D.SDRate, 0) AS SDRate,
                                ISNULL(D.DutyInTariff, 0) AS DutyInTariff,
                                ISNULL(D.ATRate, 0) AS ATRate,
                                ISNULL(D.AITRate, 0) AS AITRate,
                                ISNULL(D.VATRate, 0) AS VATRate,
                                ISNULL(D.ConversionFactorFixedValue, 0) AS ConversionFactorFixedValue,
                                ISNULL(D.VATRateFixed, 0) AS VATRateFixed,
                                ISNULL(D.RiverDues, 0) AS RiverDues,

                                ISNULL(D.TariffRate, 0) AS TariffRate,
                                ISNULL(D.FobPriceBBL, 0) AS FobPriceBBL,
                                ISNULL(D.FreightUsd, 0) AS FreightUsd,
                                ISNULL(D.ServiceCharge, 0) AS ServiceCharge,
                                ISNULL(D.ProcessFee, 0) AS ProcessFee,
                                ISNULL(D.RcoTreatmentFee, 0) AS RcoTreatmentFee,
                                ISNULL(D.AbpTreatmentFee, 0) AS AbpTreatmentFee,

                                ISNULL(D.ProcessFeeRate, 0) AS ProcessFeeRate,
                                ISNULL(D.RcoTreatmentFeeRate, 0) AS RcoTreatmentFeeRate,
                                ISNULL(D.AbpTreatmentFeeRate, 0) AS AbpTreatmentFeeRate,
                                ISNULL(D.ProductImprovementFee, 0) AS ProductImprovementFee


                                FROM ChargeDetails H
                               LEFT OUTER JOIN Products P ON D.ProductId =P.Id
                    WHERE 1= 1

        WHERE 1 = 1

    -- Add the filter condition
        " + (options.filter.Filters.Count > 0 ? " AND (" + GridQueryBuilder<ChargeDetailVM>.FilterCondition(options.filter) + ")" : "");

                // Apply additional conditions
                sqlQuery = ApplyConditions(sqlQuery, conditionalFields, conditionalValues, false);

                sqlQuery += @"
    ) AS a
    WHERE rowindex > @skip AND (@take = 0 OR rowindex <= @take)
";

                data = KendoGrid<ChargeDetailVM>.GetTransactionalGridData_CMD(options, sqlQuery, "H.Id", conditionalFields, conditionalValues);

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
            finally
            {
                if (isNewConnection && conn != null)
                {
                    conn.Close();
                }
            }
        }


    }
}
