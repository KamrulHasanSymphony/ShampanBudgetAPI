using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.KendoCommon
{
    public class GridOptions
    {
        public int skip { get; set; }
        public int take { get; set; }
        public int page { get; set; }
        public int pageSize { get; set; }
        //public List<AzFilter.GridSort> sort { get; set; } = new List<AzFilter.GridSort>();
        //public AzFilter.GridFilters filter { get; set; } = new AzFilter.GridFilters();

        public List<AzFilter.GridSort> sort { get; set; }
        public AzFilter.GridFilters filter { get; set; }

        public CommonVM vm { get; set; }
        public GridOptions()
        {
            vm = new CommonVM();
        }
    }

    public class AutoCompOptions
    {
        public int skip { get; set; }
        public int take { get; set; }
        public int page { get; set; }
        public int pageSize { get; set; }
        public AzFilter.AutoCompFilters filter { get; set; }
    }

    public class AutoCompOptions1
    {
        public int skip { get; set; }
        public int take { get; set; }
        public int page { get; set; }
        public int pageSize { get; set; }
        public AzFilter.AutoCompFilters1 filter { get; set; }
    }

    public class GridColumns
    {
        public string field { get; set; }
        public string title { get; set; }
        public string width { get; set; }
        public bool filterable { get; set; }
        public bool sortable { get; set; }
        public bool hidden { get; set; }
    }

    public class GridResult<T>
    {

        public GridEntity<T> Data(List<T> list, int totalCount)
        {
            var dEntity = new GridEntity<T>();
            dEntity.Items = list ?? new List<T>();
            dEntity.TotalCount = totalCount;
            dEntity.Columnses = new List<GridColumns>();
            return dEntity;
        }

    }

    public class GridEntity<T>
    {
        public IList<T> Items { get; set; }
        public int TotalCount { get; set; }
        public IList<GridColumns> Columnses { get; set; }

    }
}
