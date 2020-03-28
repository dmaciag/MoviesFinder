using System;

namespace MovieFinder
{
    internal static class Program
    {
        private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public static void Main(string[] args)
        {
            Logger.Info("Staring main.");

            var movieGrep = new MovieGrep();
            movieGrep.Run();

            Logger.Info("Exiting main.");
        }
    }
}