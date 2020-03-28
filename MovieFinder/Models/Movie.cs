using System;

namespace MovieFinder.Models
{
    public class Movie : MovieParam
    {
        public string Title { get; set; }
        public string Plot { get; set; }
        public string Language { get; set; }
        public string Country { get; set; }
        public string Runtime { get; set; }
        public string Genre { get; set; }
        public string Year { get; set; }
        
        public string Released { get; set; }
        public DateTime LastUpdated { get; set; }
    }
}