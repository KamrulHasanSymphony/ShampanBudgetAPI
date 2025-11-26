using Microsoft.AspNetCore.Identity;
using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.AccountVMs
{

    public class ApplicationUser : IdentityUser
    {
        public string FullName { get; set; }
        public string NormalizedName { get; set; }
        public string NormalizedUserName { get; set; }
        public DateTime? LockoutEnd { get; set; }
        public string NormalizedEmail { get; set; }
        public bool IsHeadOffice { get; set; }

        [StringLength(256)]
        public string NormalizedPassword { get; set; }

    }


}
