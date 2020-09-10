using System.Data.SqlClient;

namespace ComercioNet2Flexline
{
    class ConexionSQL
    {
        //Cadena de Conexion
        string StringConnection ="data source = ServidorSQL; initial catalog = BaseDatos; user id = Usuario; password = Contraseña";

        public SqlConnection Conectarbd = new SqlConnection();

        //Constructor
        public ConexionSQL ()
        {
        Conectarbd.ConnectionString=StringConnection;
        }

        //Metodo para abrir la conexion
        public void abrir()
        {
            try
            {
            Conectarbd.Open();
            } catch(System.Exception ex)
            {
                System.Console.WriteLine("error al  BD ",ex.Message); 
            }
        }

        //Metodo para cerrar la conexion
        public void cerrar()
        {
            Conectarbd.Close();
        }
    }
}