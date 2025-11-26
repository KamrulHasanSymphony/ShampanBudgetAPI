namespace ShampanBFRS.ViewModel.CommonVMs
{
    public class CommonVM
    {
        public string? Id { get; set; }
        public string? UserId { get; set; }
        public string? CompanyId { get; set; }
        public string? BranchId { get; set; }
        public string?[] IDs { get; set; } = Array.Empty<string?>();
        public string? ModifyBy { get; set; }
        public string? ModifyFrom { get; set; }
        public string? SettingValue { get; set; }
        public string? FromDate { get; set; }
        public string? ToDate { get; set; }
        public string? IsPost { get; set; }


        public string? Status { get; set; }
        public string? Type { get; set; }
        public string? TableName { get; set; }
        public string? Name { get; set; }
        public string? Group { get; set; }
        public string? Value { get; set; }
        public string[] ConditionalFields { get; set; } = Array.Empty<string>();
        public string[] ConditionalValues { get; set; } = Array.Empty<string>();

        public string? YearId { get; set; }
        public string? BudgetSetNo { get; set; }
        public string? BudgetType { get; set; }
        public string? TransactionType { get; set; }

    }


}
