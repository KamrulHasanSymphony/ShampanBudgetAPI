using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using ShampanBFRS.Service.SetUp;
using ShampanBFRS.ViewModel.AccountVMs;
using ShampanBFRS.ViewModel.CommonVMs;
using ShampanBFRS.ViewModel.SetUpVMs;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace ShampanBFRSAPI.Controllers.Login
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserLoginController : ControllerBase
    {
        UserProfileService _service = new UserProfileService();
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly RoleManager<IdentityRole> _roleManager;
        private readonly SignInManager<ApplicationUser> _signInManager;
        private readonly IConfiguration _configuration;


        public UserLoginController(UserManager<ApplicationUser> userManager, RoleManager<IdentityRole> roleManager, SignInManager<ApplicationUser> signInManager, IConfiguration configuration)
        {
            _userManager = userManager;
            _roleManager = roleManager;
            _signInManager = signInManager;
            _configuration = configuration;
        }


        // POST: api/UserLogin/CreateEditAsync 
        [HttpPost("CreateEditAsync")]
        public async Task<ResultVM> CreateEditAsync(UserProfileVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                var claims = new List<Claim>
                    {
                        new Claim("Database", "Tailor_DB"),
                    };

                if (model.Operation == "update")
                {
                     var user = await _userManager.FindByNameAsync(model.UserName);

                    if (user == null)
                    {
                        resultVM.Message = "User not found.";
                        return resultVM;
                    }
                    if (!string.IsNullOrEmpty(model.Password) && !string.IsNullOrEmpty(model.ConfirmPassword) && model.Mode == "profileupdate")
                    {
                        // Update the user profile without changing the password
                        user.Email = model.Email;
                        user.PhoneNumber = model.PhoneNumber;
                        user.FullName = model.FullName;
                        user.IsHeadOffice = model.IsHeadOffice;

                        var updateResult = await _userManager.UpdateAsync(user);

                        if (!updateResult.Succeeded)
                        {
                            resultVM.Message = "Failed to update user profile.";
                            return resultVM;
                        }

                        var _userInfo = new UserInformationVM
                        {
                            UserId =model.Id,
                            UserName = model.UserName,
                            FullName = model.FullName,
                            DepartmentId = model.DepartmentId,
                            CreatedBy = model.CreatedBy,
                            CreatedFrom = model.CreatedFrom,
                            CreatedOn = model.CreatedOn,
                            CreatedAt = model.CreatedOn,
                            IsActive = true,
                            IsArchive = false,

                        };

                        resultVM = await _service.UserInformationsUpdate(_userInfo);

                        resultVM.Status =MessageModel.Success;
                        resultVM.Message = MessageModel.UpdateSuccess;
                        resultVM.DataVM = model;
                        return resultVM;
                    }

                    if (User.IsInRole("Admin"))
                    {
                        string token = "";
                        if (user != null)
                        {
                            token = await _userManager.GeneratePasswordResetTokenAsync(user);

                            var changePasswordResult = await _userManager.ResetPasswordAsync(user, token, model.Password);

                            if (!changePasswordResult.Succeeded)
                            {
                                resultVM.Message = "Failed to change the password.";
                                return resultVM;
                            }
                        }
                    }
                    else
                    {
                        var changePasswordResult = await _userManager.ChangePasswordAsync(user, model.CurrentPassword, model.Password);

                        if (!changePasswordResult.Succeeded)
                        {
                            resultVM.Message = "Failed to change the password.";
                            return resultVM;
                        }
                    }

                    resultVM.Status = "Success";
                    resultVM.Message = "Password successfully updated.";
                    resultVM.DataVM = model;

                    return resultVM;
                }
                else
                {

                    if (model.Password != model.ConfirmPassword)
                    {
                        resultVM.Message = "Passwords do not match!";
                        return resultVM;
                    }

                    var _user = new ApplicationUser
                    {
                        UserName = model.UserName,
                        FullName = model.FullName,
                        PhoneNumber = model.PhoneNumber
                        ,
                        Email = model.Email,
                        EmailConfirmed = false,
                        PhoneNumberConfirmed = false,
                        TwoFactorEnabled = false
                        ,
                        LockoutEnabled = false,
                        AccessFailedCount = 0,
                        NormalizedName = model.UserName,
                        NormalizedUserName = model.UserName
                        ,
                        NormalizedEmail = model.Email,
                        NormalizedPassword = model.Password,
                        IsHeadOffice = model.IsHeadOffice
                    };

                    var _result = await _userManager.CreateAsync(_user, model.Password);

                     if (!_result.Succeeded)
                    {
                        foreach (var error in _result.Errors)
                        {
                            resultVM.Message = error.Description;
                            return resultVM;
                        }
                    }
                    var userClaimsresult = await _userManager.AddClaimsAsync(_user, claims);

                    if (!userClaimsresult.Succeeded)
                    {
                        resultVM.Message = "Failed to add claims.";
                        return resultVM;
                    }

                    // Ensure the default role exists
                    await EnsureRoleExistsAsync(DefaultRoles.Role);
                    // Assign the default role to the new user
                    var addRolesResult = await _userManager.AddToRoleAsync(_user, DefaultRoles.Role);
                    if (!addRolesResult.Succeeded)
                    {
                        resultVM.Message = "Failed to assign default role.";
                        return resultVM;
                    }

                    // Add external login if provided
                    var loginuser = await _userManager.FindByNameAsync(model.UserName);

                    if (loginuser != null)
                    {
                        var userLoginInfo = new UserLoginInfo(
                            loginProvider: "Google",
                            providerKey: DateTime.Now.ToString("yyyMMddHHmmss"),
                            displayName: "Google"
                        );

                        var addLoginResult = await _userManager.AddLoginAsync(_user, userLoginInfo);
                        if (!addLoginResult.Succeeded)
                        {
                            resultVM.Message = "Failed to add external login.";
                            return resultVM;
                        }
                    }

                    var _userInfo = new UserInformationVM
                    {
                        UserId = loginuser.Id,
                        UserName = model.UserName,
                        FullName = model.FullName,
                        DepartmentId = model.DepartmentId,
                        CreatedBy = model.CreatedBy,
                        CreatedFrom = model.CreatedFrom,
                        CreatedOn = model.CreatedOn,
                        CreatedAt = model.CreatedOn,
                        IsActive = true,
                        IsArchive = false,

                    };

                    resultVM = await _service.UserInformationsInsert(_userInfo);

                    resultVM.Status = MessageModel.Success;
                    resultVM.Message =MessageModel.InsertSuccess;
                    model.Id = _user.Id;
                    resultVM.DataVM = model;
                }
            }
            catch (Exception ex)
            {
                resultVM.Message = ex.Message;
                resultVM.ExMessage = ex.Message;
            }

            return resultVM;
        }

        private async Task EnsureRoleExistsAsync(string roleName)
        {
            var roleExists = await _roleManager.RoleExistsAsync(roleName);
            if (!roleExists)
            {
                var role = new IdentityRole(roleName);
                var result = await _roleManager.CreateAsync(role);
                if (!result.Succeeded)
                {
                    throw new Exception($"Failed to create role: {roleName}");
                }
            }
        }

        private AuthVM GetAccessToken(LoginResourceVM model)
        {
            if (string.IsNullOrWhiteSpace(model.UserName) || string.IsNullOrWhiteSpace(model.Password))
            {
                return new AuthVM
                {
                    token = "Invalid username or password",
                    Token_type = "None",
                    Expires_in = "0"
                };
            }

            var claims = new[]
            {
                new Claim(JwtRegisteredClaimNames.Sub, model.UserName),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
            };

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["Jwt:SecretKey"]));
            var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

            var expiration = DateTime.UtcNow.AddHours(24);

            var token = new JwtSecurityToken(
                issuer: _configuration["Jwt:Issuer"],
                audience: _configuration["Jwt:Audience"],
                claims: claims,
                expires: expiration,
                signingCredentials: creds);

            var tokenHandler = new JwtSecurityTokenHandler();
            var jwtToken = tokenHandler.WriteToken(token);

            return new AuthVM
            {
                token = jwtToken,
                Token_type = "Bearer",
                Expires_in = expiration.ToString("yyyy-MM-dd HH:mm ss") // ISO 8601 format
            };
        }


        // POST: api/UserLogin/SignIn
        [HttpPost("SignIn")]
        public async Task<ResultVM> SignIn(LoginResourceVM model)
        {
            ResultVM resultVM = new ResultVM { Status = "Fail", Message = "Error", ExMessage = null, Id = "0", DataVM = null };
            try
            {
                var result = await _signInManager.PasswordSignInAsync(model.UserName, model.Password, false, false);

                if (!result.Succeeded)
                {
                    resultVM.Message = "Wrong username or password";
                    return resultVM;
                }

                var user = _userManager.Users.SingleOrDefault(x => x.UserName == model.UserName);

                if (user == null)
                {
                    resultVM.Message = "Wrong username or password";
                    return resultVM;
                }

                var tokenResult = GetAccessToken(model);

                resultVM.Status = "Success";
                resultVM.Message = "Login Successfully.";
                resultVM.Data = tokenResult;
                resultVM.DataVM = user;
            }
            catch (Exception ex)
            {
                resultVM.Message = ex.Message;
                resultVM.ExMessage = ex.Message;
            }

            return resultVM;
        }



    }
}
