namespace ShampanBFRS.ViewModel.KendoCommon
{
    public class GridFilterHelper
    {
        public static List<GridFilterCondition> FormatDateTimeFilter(List<GridFilterCondition> filters)
        {
            return filters.Select(f =>
            {
                if (f.Value is DateTime dateTimeValue)
                {
                    return new GridFilterCondition
                    {
                        Field = f.Field,
                        Operator = f.Operator,
                        Value = dateTimeValue.ToString("yyyy-MM-dd")
                    };
                }
                return f;
            }).ToList();
        }
    }
}
