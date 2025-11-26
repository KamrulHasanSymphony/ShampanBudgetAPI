using ShampanBFRS.ViewModel.CommonVMs;
using System;

namespace ShampanBFRS.ViewModel.QuestionVM
{
    public class QuestionChapterVM : AuditVM
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? NameInBangla { get; set; }
        public string? Remarks { get; set; }

        public PeramModel PeramModel { get; set; }

        public QuestionChapterVM()
        {
            PeramModel = new PeramModel();
        }
    }
}
