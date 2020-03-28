using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using log4net.Repository.Hierarchy;
using MovieFinder.Enums;
using MovieFinder.Models;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Bcpg;

namespace MovieFinder
{
    public class MovieGrep
    {
        private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly string ReplaceImdbIdFlag = "ReplaceImdbIdFlag";
        private static readonly string APIKeyReplaceFlag = "APIKeyReplaceFlag";

        private readonly string[] OmdbApiKeys = new string[] {"396cfc8d","b3b1c3ff","23e3cce6","8282b935", "7bf4c1bb","1efc2591","ae793fb6"};
        private readonly string OmdbApiUrlBase = $"https://www.omdbapi.com/?i={ReplaceImdbIdFlag}&apikey={APIKeyReplaceFlag}";

        private readonly string _connectionString = "Data Source=localhost;Initial Catalog=imdb;User ID=root;Password=";
        private readonly string FilePathToImdbDataSet = @"C:\Users\JOHNC\Documents\coding_projects\C#\MoviesFinder\MovieFinder\movie_dataset_imdb\data.tsv";

        //https://datasets.imdbws.com/title.ratings.tsv.gz
        public void Run()
        {
            var movieParamList = GetMovieParamsFromImdbDump(7.5M, 10000);
            Logger.Info($"Found {movieParamList} movie params from imdb dump.");

            if (movieParamList == null || movieParamList.Count == 0)
            {
                Logger.Warn("Bad movie param list. Not proceeding with Get.");
                return;
            }

            var moviesInDb = GetMoviesFromDb();
            RemoveAlreadyExsitingItemsFromSearch(movieParamList, moviesInDb);
            
            MySqlConnection conn = new MySqlConnection(_connectionString);
            conn.Open();
            foreach (var movie in GetMoviesFromImdbApi(movieParamList))
            {
                Parse(movie, conn, moviesInDb);
            }

            conn.Close();
            Console.WriteLine("Done everything.");
        }

        private void RemoveAlreadyExsitingItemsFromSearch(IList<MovieParam> movieParamList, Dictionary<string, Movie> moviesInDb)
        {
            IList<int> imdbIndxsToSkipInsert = new List<int>();
            
            if (true)
            {
                int indx = 0;
                foreach (var movieParam in movieParamList)
                {
                    if (moviesInDb.ContainsKey(movieParam.ImdbId))
                    {
                        imdbIndxsToSkipInsert.Add(indx);
                    }

                    indx++;
                }

                for (int i = imdbIndxsToSkipInsert.Count-1; i>=0 ; i--)
                {
                    movieParamList.RemoveAt(imdbIndxsToSkipInsert[i]);
                }
            }
        }

        private void Parse(Movie movieImdb, MySqlConnection conn, Dictionary<string, Movie> moviesDb)
        {
            int updateRecordCount = 0;
            int insertRecordCount = 0;
            
            try
            {
                if (moviesDb.TryGetValue(movieImdb.ImdbId, out Movie movieInDb) &&
                    movieInDb.LastUpdated.AddDays(7) < DateTime.Now)
                {
                    Logger.Debug($"Attempting to update movie record for ImdbId:{movieImdb.ImdbId}.");

                    if (UpdateMovieRecord(movieImdb, conn))
                        updateRecordCount++;
                }

                if (InsertMovieRecord(movieImdb, conn))
                    insertRecordCount++;
            }
            catch (Exception ex)
            {
                Logger.Error($"Error during Parse. ex: {ex}.");
            }
            finally
            {
                conn?.Close();
            }
            
            Logger.Info($"Finished Parse. Updated {updateRecordCount} records, Inserted {insertRecordCount} records.");
        }

        private Dictionary<string, Movie> GetMoviesFromDb()
        {
            Dictionary<string, Movie> moviesFromDb = new Dictionary<string, Movie>();

            MySqlConnection conn = null;
            try
            {
                conn = new MySqlConnection(_connectionString);
                conn.Open();
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = conn;
                cmd.CommandText = "SELECT * FROM movies";

                MySqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    Movie movieFromDb = new Movie();
                    movieFromDb.ImdbId = reader["ImdbId"].ToString();
                    movieFromDb.Plot = reader["Plot"].ToString();
                    if (Decimal.TryParse(reader["Rating"].ToString(), out Decimal rating))
                        movieFromDb.Rating = rating;
                    if (Int32.TryParse(reader["VoteCount"].ToString(), out Int32 voteCount))
                        movieFromDb.VoteCount = voteCount;
                    if (DateTime.TryParse(reader["LastUpdated"].ToString(), out DateTime lastUpdated))
                        movieFromDb.LastUpdated = lastUpdated;

                    if (!moviesFromDb.ContainsKey(movieFromDb.ImdbId))
                        moviesFromDb.Add(movieFromDb.ImdbId, movieFromDb);
                    else
                        Logger.Warn($"Found duplicate ImdbId:{movieFromDb.ImdbId} in Db.");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error during GetMoviesFromDb(), ex: {ex}.");
            }
            finally
            {
                conn?.Close();
            }
            
            Logger.Info($"Found {moviesFromDb.Count} movies from db.");

            return moviesFromDb;
        }

        private IEnumerable<Movie> GetMoviesFromImdbApi(IList<MovieParam> movieParams)
        {
            IEnumerable<Movie> movies = new List<Movie>();
            
            int maxNumKeys = OmdbApiKeys.Length;
            int currentIndexKey = 0;
            int _maxRetryCount = 0;
            string currentApiKey = OmdbApiKeys[currentIndexKey];

            HashSet<int> pctTrackerSet = new HashSet<int>();
            for (int i=0;i<movieParams.Count; i++)
            {
                PrintStatus(i, movieParams.Count, pctTrackerSet);
                
                if (currentIndexKey >= maxNumKeys)
                    continue;
                
                Thread.Sleep(15);
                MovieParam currentMovieParam = movieParams[i];
                int retryCount = 0;
                RequestStatus requestStatus = RequestStatus.Fail;

                Movie movie = TryGetMovieFromImdbHttp(currentMovieParam, currentApiKey, ref requestStatus);
                
                while (requestStatus == RequestStatus.Fail && retryCount < _maxRetryCount)
                {
                    Thread.Sleep(100);
                    Logger.Info($"Retrying search for ImdbId:{currentMovieParam.ImdbId} due to {requestStatus}.");
                    movie = TryGetMovieFromImdbHttp(currentMovieParam, currentApiKey,  ref requestStatus);
                    retryCount++;
                }
                
                while (requestStatus == RequestStatus.HitApiLimit && currentIndexKey < maxNumKeys - 1)
                {
                    currentIndexKey++;
                    Logger.Info($"API Limit reached for ApiKey:{currentApiKey}. Trying ApiKey:{OmdbApiKeys[currentIndexKey]}.");
                    currentApiKey = OmdbApiKeys[currentIndexKey];
                    Logger.Info($"Retrying search for ImdbId:{currentMovieParam.ImdbId} due to {requestStatus} with ApiKey:{currentApiKey}.");
                    movie = TryGetMovieFromImdbHttp(currentMovieParam, currentApiKey,  ref requestStatus);
                }

                if (requestStatus == RequestStatus.HitApiLimit && currentIndexKey == maxNumKeys - 1)
                {
                    currentIndexKey++; //last one
                }

                retryCount = 0;
                while (requestStatus == RequestStatus.Wait && retryCount < _maxRetryCount)
                {
                    Logger.Info("Waiting for 5 seconds before retrying.");
                    retryCount++;
                    Thread.Sleep(5000);
                }

                if (movie != null)
                {
                    yield return movie;
                }
                else
                    Logger.Warn($"Skipping movie with ImdbId:{currentMovieParam.ImdbId} due to reason:{requestStatus}.");
            }
        }

        private void PrintStatus(int i, int maxCount, HashSet<int> pctTrackerSet)
        {
            int intOutOf100 = i * 100 / maxCount;
            if (!pctTrackerSet.Contains(intOutOf100))
            {
                Console.WriteLine($"Done {intOutOf100}.");
                pctTrackerSet.Add(intOutOf100);
            }
        }

        private bool InsertMovieRecord(Movie movie, MySqlConnection conn)
        {
            if (conn == null)
            {
                Logger.Error("Null connection, not saving.");
                return false;
            }

            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }

            if (movie == null)
            {
                Logger.Error("Null movie, not saving.");
                return false;
            }
            
            try
            {
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = conn;
                cmd.CommandText = "INSERT INTO movies(ImdbId,Title,Plot,Rating,VoteCount,Language,Country,Genre,Year,Released,Runtime,Type,LastUpdated) " +
                                  "VALUES(@ImdbId,@Title,@Plot,@Rating,@VoteCount,@Language,@Country,@Genre,@Year,@Released,@Runtime,@Type,@LastUpdated)";

                cmd.Parameters.AddWithValue("@ImdbId", movie.ImdbId);
                cmd.Parameters.AddWithValue("@Title", movie.Title);
                cmd.Parameters.AddWithValue("@Plot", movie.Plot) ;
                cmd.Parameters.AddWithValue("@Rating", movie.Rating);
                cmd.Parameters.AddWithValue("@VoteCount", movie.VoteCount);
                cmd.Parameters.AddWithValue("@Language", movie.Language);
                cmd.Parameters.AddWithValue("@Country", movie.Country);
                cmd.Parameters.AddWithValue("@Genre", movie.Genre);
                cmd.Parameters.AddWithValue("@Runtime", movie.Runtime);
                cmd.Parameters.AddWithValue("@Released", movie.Released);
                cmd.Parameters.AddWithValue("@Year", movie.Year);
                cmd.Parameters.AddWithValue("@Type", movie.Type);
                cmd.Parameters.AddWithValue("@LastUpdated", DateTime.Now);

                cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to InsertMovie:{movie.ImdbId} to DB, ex: {ex}.");
            }

            return false;
        }

        private bool UpdateMovieRecord(Movie movie, MySqlConnection conn)
        {
            if (conn == null)
            {
                Logger.Error("Null connection, not saving.");
                return false;
            }

            if (movie == null)
            {
                Logger.Error("Null movie, not saving.");
                return false;
            }
            
            try
            {
                MySqlCommand cmd = new MySqlCommand();
                cmd.Connection = conn;
                cmd.CommandText = "UPDATE movies " +
                                  "SET Title=@Title,Description=@Description,Rating=@Rating,VoteCount=@VoteCount,LastUpdated=@LastUpdated," +
                                  "Language=@Language,Country=@Country,Genre=@Genre,Runtime=@Runtime,Released=@Released,Year=@Year,Type=@Type" +
                                  "WHERE ImbdId=@ImbdId";

                cmd.Parameters.AddWithValue("@ImbdId", movie.ImdbId);
                cmd.Parameters.AddWithValue("@Title", movie.Title);
                cmd.Parameters.AddWithValue("@Plot", movie.Plot);
                cmd.Parameters.AddWithValue("@Rating", movie.Rating);
                cmd.Parameters.AddWithValue("@VoteCount", movie.VoteCount);
                cmd.Parameters.AddWithValue("@LastUpdated", DateTime.Now);
                cmd.Parameters.AddWithValue("@Language", movie.Language);
                cmd.Parameters.AddWithValue("@Country", movie.Country);
                cmd.Parameters.AddWithValue("@Genre", movie.Genre);
                cmd.Parameters.AddWithValue("@Runtime", movie.Runtime);
                cmd.Parameters.AddWithValue("@Released", movie.Released);
                cmd.Parameters.AddWithValue("@Year", movie.Year);
                cmd.Parameters.AddWithValue("@Type", movie.Type);
                
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to LastUpdated:{movie.ImdbId} to DB, ex: {ex}.");
            }

            return false;
        }

        private Movie TryGetMovieFromImdbHttp(MovieParam movieParam, string currentApiKey, ref RequestStatus requestStatus)
        {
            Movie movie = new Movie();
            requestStatus = RequestStatus.Fail;

            try
            {
                string url = OmdbApiUrlBase.Replace(APIKeyReplaceFlag, currentApiKey)
                    .Replace(ReplaceImdbIdFlag, movieParam.ImdbId);
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                //request.AutomaticDecompression = DecompressionMethods.GZip;
                request.Method = "GET";
                request.Accept = "application/json";

                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                using (Stream stream = response.GetResponseStream())
                using (StreamReader reader = new StreamReader(stream))
                {
                    string jsonString = reader.ReadToEnd();
                    
                    Movie httpMovieData = JsonConvert.DeserializeObject<Movie>(jsonString);
                    movie.Genre = httpMovieData.Genre;
                    movie.Plot = httpMovieData.Plot;
                    movie.Language = httpMovieData.Language;                    
                    movie.Country = httpMovieData.Country;
                    movie.Year = httpMovieData.Year;
                    movie.Released = httpMovieData.Released;
                    movie.Runtime = httpMovieData.Runtime;
                    movie.Title = httpMovieData.Title;
                    movie.Type = httpMovieData.Type;
                    movie.ImdbId = movieParam.ImdbId;
                    movie.Rating = movieParam.Rating;
                    movie.VoteCount = movieParam.VoteCount;
                    requestStatus = RequestStatus.Success;
                }

                return movie;
            }
            catch (Exception ex)
            {
                requestStatus = RequestStatus.Fail;

                if (ex.ToString().Contains("401"))
                    requestStatus = RequestStatus.HitApiLimit;
                Logger.Debug(Convert.ToString(ex));
            }
            
            
            return null;
        }

        private IList<MovieParam> GetMovieParamsFromImdbDump(decimal minRating = 7.5M, int minVoteCount = 10000)
        {
            IList<MovieParam> movieParams = new List<MovieParam>();
            try
            {
                const Int32 bufferSize = 128;
                using (var fileStream = File.OpenRead(FilePathToImdbDataSet))
                {
                    int lineCount = 0;
                    using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, bufferSize))
                    {
                        string line;
                        while ((line = streamReader.ReadLine()) != null)
                        {
                            //Debug.WriteLine("PointModel Get LineModel {0} ", lineCount);
                            lineCount++;
                            if (IsValidImdbParam(line, out var movieParam))
                            {
                                if(movieParam.Rating >= minRating && movieParam.VoteCount >= minVoteCount)
                                    movieParams.Add(movieParam);
                            }
                            else
                            {
                                Logger.Error($"MovieParam {lineCount} is not a valid movieParam.");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Read from File exception: {ex}.");
            }

            return movieParams;
        }

        private bool IsValidImdbParam(string line, out MovieParam movieParam)
        {
            movieParam = new MovieParam();
            if (string.IsNullOrEmpty(line))
                return false;


            string[] entryColumns = line.Split('\t');
            if (entryColumns.Length != 3)
                return false;

            if (Decimal.TryParse(entryColumns[1], out Decimal rating))
            {
                movieParam.Rating = rating;
            }
            else
            {
                return false;
            }
            
            if (Int32.TryParse(entryColumns[2], out Int32 voteCount))
            {
                movieParam.VoteCount = voteCount;
            }
            else
            {
                return false;
            }
            
            if (!string.IsNullOrEmpty(entryColumns[0]) || !entryColumns[0].StartsWith("tt"))
            {
                movieParam.ImdbId = entryColumns[0];
            }
            else
            {
                return false;
            }

            return true;
        }
    }
}