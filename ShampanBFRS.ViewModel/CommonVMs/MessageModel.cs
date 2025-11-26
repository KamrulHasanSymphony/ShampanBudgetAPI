namespace ShampanBFRS.ViewModel.CommonVMs
{
    public class MessageModel
    {
        public static string Success { get; set; } = "Success";
        public static string Fail { get; set; } = "Fail";

        public static string RetrievedSuccess { get; set; } = "Grid data retrieved successfully.";

        public static string InsertSuccess { get; set; } = "Data Saved Successfully";
        public static string UpdateSuccess { get; set; } = "Data Updated Successfully";
        public static string DeleteSuccess { get; set; } = "Data Deleted Successfully";
        public static string PushSuccess { get; set; } = "Data Pushed Successfully";
        public static string PostSuccess { get; set; } = "Data Posted Successfully";
        public static string UnPostSuccess { get; set; } = "Data UnPosted Successfully";
        public static string DbUpdateSuccess { get; set; } = "DbUpdate Successfully";
        public static string ApproveSuccess { get; set; } = "Data Approve Successfully";
        public static string ApproveFail { get; set; } = "Data Approve Fail.";

        public static string RejectSuccess { get; set; } = "Data Reject Successfully";
        public static string RejectFail { get; set; } = "Data Reject Fail.";

        public static string SyncSuccess { get; set; } = "Sync Successfully.";
        public static string SyncFail { get; set; } = "Sync Fail.";



        public static string ExcelSuccess { get; set; } = "Excel file generated successfully.";
        public static string ExcelFail { get; set; } = "Failed to generate Excel file.";

        public static string SubmissionFail { get; set; } = "Submission Failed";
        public static string SubmissionSuccess { get; set; } = "Submission Successful. " ;


        public static string InsertFail { get; set; } = "Insert Failed";
        public static string UpdateFail { get; set; } = "Update Failed";
        public static string DeleteFail { get; set; } = "Delete Failed";
        public static string PostFail { get; set; } = "Data Posted Failed";
        public static string UnPostFail { get; set; } = "Data UnPosted Failed";
        public static string PushFail { get; set; } = "Data Pushed Failed";
        public static string DbUpdateFail { get; set; } = "DbUpdate Failed";
        public static string OutofStock { get; set; } = "Out of Stock";
        public static string DataMappingNotFound { get; set; } = "Data Mapping Not Found!";

        public static string MasterInsertFailed { get; set; } = "Master Insert Failed";
        public static string DetailInsertFailed { get; set; } = "Details Insert Failed";
        public static string NotFoundForSave { get; set; } = "Data Not Found for Save";
        public static string NotFoundForUpdate { get; set; } = "Data Not Found for Update";
        public static string DataLoaded { get; set; } = "Data Loaded Successfully";
        public static string FiscalYear { get; set; } = "Please Setup Fiscal Year.";
        public static string DataLoadedFailed { get; set; } = "Data Not Found!";
        public static string BatchCreateFailed { get; set; } = "Batch Creation Failed!";
        public static string DetailsNotFoundForSave { get; set; } = " Details Data Not Found for Save";

        public static string AlreadyExists { get; set; } = "Already Exists";

        public static string PostAlready { get; set; } = "Data has Already been Posted";
        public static string PushAlready { get; set; } = "Data Already Pushed";
        public static string NotPost { get; set; } = "Please Data Post First!";

    }


    public class TableName
    {
        public static string CompanyInfo { get; set; } = "CompanyInfo";

    }


}
