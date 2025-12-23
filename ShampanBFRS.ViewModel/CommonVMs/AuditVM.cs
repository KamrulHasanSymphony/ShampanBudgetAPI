namespace ShampanBFRS.ViewModel.CommonVMs
{
    public class AuditVM
    {
        public string? CreatedBy { get; set; }
        public string? CreatedOn { get; set; }
        public string? CreatedAt { get; set; }
        public string? CreatedFrom { get; set; }

        public string? LastModifiedBy { get; set; }
        public string? LastUpdateBy { get; set; }
        public string? LastModifiedOn { get; set; }
        public string? LastUpdateOn { get; set; }
        public string? LastUpdateAt { get; set; }
        public string? LastUpdateFrom { get; set; }

        public string? PostedBy { get; set; }
        public string? PosterBy { get; set; }
        public string? PostedAt { get; set; }
        public string? PostedOn { get; set; }
        public string? PostedFrom { get; set; }

        public string? PushedBy { get; set; }
        public string? PushedOn { get; set; }
        public string? PushedFrom { get; set; }

        public string? Operation { get; set; }
        public string? Status { get; set; }
        public bool IsArchive { get; set; }
        public bool IsActive { get; set; }


    }


}
