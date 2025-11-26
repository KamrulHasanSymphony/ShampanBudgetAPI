using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.QuestionVM
{
    public class QuestionSetDetailVM : AuditVM
    {
        public int Id { get; set; }
        public int? QuestionSetHeaderId { get; set; }
        public int? QuestionHeaderId { get; set; }
        public int? QuestionMark { get; set; }

        public string? QuestionSetHeaderName { get; set; }  
        public string? QuestionHeaderName { get; set; }      

        public PeramModel PeramModel { get; set; }

        public QuestionSetDetailVM()
        {
            PeramModel = new PeramModel();
        }
    }
}
