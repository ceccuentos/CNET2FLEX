using System;
using System.IO;
using System.Security;

// Ejemplo Estructura de Log
// [Fri, 13 Sep 2019 15:34:07 GMT] [DEBUG]: [pincenter][PincenterService] init
// [Fri, 13 Sep 2019 15:34:07 GMT] [DEBUG]: [pincenter][PincenterService] setTimeOut:  12000
// [Fri, 13 Sep 2019 15:34:19 GMT] [DEBUG]: [pincenter][PincenterService] ***** 
// [Mon, 16 Sep 2019 20:56:24 GMT] [INFO]: [operadores] return 200 OK

namespace ComercioNet2Flexline
{
    public class App_log
    {
        public App_log(string Ruta)
        {
            this.Ruta = Ruta;
        }

        public void Add(string level, string sLog)
        {
            if (level == "CECHEADER" || level == "CECDETALLE") 
            {

            CreateDirectory();
            string nombre = GetNameFile();

            //string cadena = String.Format("[{0}] {1} - {2}{3}", DateTime.Now, level, sLog, Environment.NewLine);
            string cadena = String.Format("{0}{1}",sLog, Environment.NewLine);

            if (Transporte == "All" || Transporte == "F")
            {
                StreamWriter sw = new StreamWriter(Path.Combine(Ruta, nombre), true);
                sw.Write(cadena);
                sw.Close();
            }
            if (Transporte == "All" || Transporte == "C")
            {
                Console.WriteLine(cadena);
            }

             }
        }

        #region HELPER
        private string GetNameFile()
        {
            string nombre = "CNET2FLEXlog_" + DateTime.Now.Year + "_" + DateTime.Now.Month + "_" + DateTime.Now.Day + ".log";
            return nombre;
        }

        private void CreateDirectory()
        {
            try
            {
                if (!Directory.Exists(Ruta))
                    Directory.CreateDirectory(Ruta);
            }
            catch (Exception ex)
            {
                if
                  (
                      ex is UnauthorizedAccessException
                      || ex is ArgumentNullException
                      || ex is PathTooLongException
                      || ex is DirectoryNotFoundException
                      || ex is NotSupportedException
                      || ex is ArgumentException
                      || ex is SecurityException
                      || ex is IOException
                  )
                {
                    throw new Exception(ex.Message);
                }
            }
        }
        #endregion

        private string Ruta = "";
        private string Transporte = "F";  // C: Consola  F: Archivo All: Ambos
    }
}
