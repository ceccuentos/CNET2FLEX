using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Xml.Linq;
using System.Data.SqlClient;
using System.Reflection;
using System.Xml;

namespace ComercioNet2Flexline
{
    public class PrimeService
    {
        public bool IsPrime(int candidate)
        {
            if (candidate == 1)
            {
                return false;
            }
            throw new NotImplementedException("Not implemented.");
        }
    }
    class CNET2Flex
    {
        public CNET2Flex()
        {
            // Get Params
            try
            {
                Params.GLNlocacion = new List<GLN>();
                Params.RutSociedades = new List<string[]>();
                Params.CtacteComercioNet2Flex = new List<string[]>();

                var filenameXMLSettings = "CNET2FLEX_Config.xml";
                var currentDirectory = Path.Combine(@AppDomain.CurrentDomain.BaseDirectory, "Logs");
                var settingsXMLFilepath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, filenameXMLSettings);

                oLog = new App_log(currentDirectory);
                oLog.Add("DEBUG", "======== Inicio Proceso ========");
                oLog.Add("TRACE", "Lee Configuraciones: " + settingsXMLFilepath);

                if (!File.Exists(settingsXMLFilepath))
                {
                    throw new System.ArgumentException("No existe archivo de configuración", settingsXMLFilepath);
                }

                XElement Properties_Settings = XElement.Load(settingsXMLFilepath);
                IEnumerable<XElement> nodeSetts = from parametro in Properties_Settings.Descendants("CNET2FLEX.Properties.Settings")
                                                  select (XElement)parametro;


                foreach (XElement elemento in nodeSetts.Elements())
                {
                    // Tratamiento especial array nodos GLN y lista de sociedades
                    if (elemento.Attribute("name").Value == "GLN")
                    {
                        XElement nodoGLN = XElement.Parse(elemento.FirstNode.ToString());

                        IEnumerable<XElement> elementoGLN = from xel in nodoGLN.Descendants("ArrayOfString")
                                                                 select (XElement)xel;
                        foreach (XElement elxnt in elementoGLN.Elements())
                        {
                            GLN GLNx = new GLN();
                            GLNx.GLNDespacho = elxnt.Value.Split(';')[0];
                            GLNx.Nombre = elxnt.Value.Split(';')[1];
                            GLNx.Direccion = elxnt.Value.Split(';')[2];
                            GLNx.Comuna = elxnt.Value.Split(';')[3];

                            Params.GLNlocacion.Add(GLNx);
                        }
                    }
                    
                    if (elemento.Attribute("name").Value == "RutSociedades")
                    {
                        XElement nodoSoc = XElement.Parse(elemento.FirstNode.ToString());

                        IEnumerable<XElement> elementoSociedad = from xel in nodoSoc.Descendants("ArrayOfString")
                                                                 select (XElement)xel;
                        foreach (XElement elxnt in elementoSociedad.Elements())
                        {
                            Params.RutSociedades.Add(elxnt.Value.Split(';'));
                        }
                    }
                    if (elemento.Attribute("name").Value == "CtacteComercioNet2Flex")
                    {
                        XElement nodoSoc = XElement.Parse(elemento.FirstNode.ToString());

                        IEnumerable<XElement> elementoSociedad = from xel in nodoSoc.Descendants("ArrayOfString")
                                                                 select (XElement)xel;
                        foreach (XElement elxnt in elementoSociedad.Elements())
                        {
                            Params.CtacteComercioNet2Flex.Add(elxnt.Value.Split(';'));
                        }
                    }

                    // Otros Nodos
                    switch (elemento.Attribute("name").Value)
                    {
                        case "FTPServer":
                            Params.FTPServer = new Uri(elemento.Value);
                            break;
                        case "FTPPort":
                            Params.FTPPort = elemento.Value;
                            break;
                        case "FTPUser":
                            Params.FTPUser = elemento.Value;
                            break;
                        case "FTPPassword":
                            Params.FTPPassword = elemento.Value;
                            break;
                        case "SMTPName":
                            Params.SMTPName = elemento.Value;
                            break;
                        case "SMTPPort":
                            Params.SMTPPort = Convert.ToInt32(elemento.Value);
                            break;
                        case "EnableSSL":
                            Params.EnableSSL = Convert.ToBoolean(elemento.Value);
                            break;
                        case "EmailUser":
                            Params.EmailUser = elemento.Value;
                            break;
                        case "EmailPassword":
                            Params.EmailPassword = elemento.Value;
                            break;
                        case "EmailTO":
                            Params.EmailTO = elemento.Value;
                            break;
                        case "EmailTO2":
                            Params.EmailTO2 = elemento.Value;
                            break;
                        case "DirectorioFTPCNET":
                            Params.DirectorioFTPCNET = elemento.Value;
                            break;
                        case "StringConexionSisPal":
                            Params.StringConexionSisPal = elemento.Value;
                            break;
                        case "StringConexionFlexline":
                            Params.StringConexionFlexline = elemento.Value;
                            break;                            

                    }
                }
                
                // Validaciones
                bool Valid = true;
                if (String.IsNullOrEmpty(Params.EmailUser) || String.IsNullOrEmpty(Params.EmailTO) || String.IsNullOrEmpty(Params.SMTPName) )
                {
                    oLog.Add("ERROR", "Faltan datos para envío de Email (User, To, To2, Pass u otro.)");
                    Valid = false;
                }
                if (Params.RutSociedades.Count == 0)
                {
                    oLog.Add("ERROR", "Sociedades no encontradas, revise estructura XML");
                    Valid = false;
                }
                if (!VerifyConnectionSQL(Params.StringConexionFlexline))
                {
                    oLog.Add("ERROR", "No fue posible conectar con BD.");
                    Valid = false;
                }
                if (!Directory.Exists(Params.DirectorioFTPCNET)) //String.IsNullOrEmpty(Params.DirectorioFTPCNET) )
                {
                    oLog.Add("ERROR", "Directorio de trabajo no existe o no está definido");
                    Valid = false;
                }
                if (Valid)
                {
                    oLog.Add("TRACE", "Configuración leída con éxito");
                } else
                {
                    oLog.Add("TRACE", "Problemas con lectura de configuración");
                    throw new System.ArgumentException("Parametros incorrectos");
                }
            }
            catch (Exception ex)
            {
                oLog.Add("ERROR", ex.Message);
                throw new Exception(ex.Message);
            }
        }
        static void Main(string[] args)
        {
         try 
            {
                CNET2Flex C2F = new CNET2Flex();
                
                IEnumerable<String> filesXmlinDirCNET = Directory.EnumerateFiles(@Params.DirectorioFTPCNET, "*.*");
                oLog.Add("TRACE", String.Format("{0} archivos encontrados ", filesXmlinDirCNET.Count()));
                
                int Correlativo = 1;  
                foreach (var file in filesXmlinDirCNET)
                {
                    string onlyfileName = file.Substring(Params.DirectorioFTPCNET.Length + 1).Replace(".xml", "");
                    string[] ArrayFileName = onlyfileName.Split(".");

                    if (isFileXML(file))
                    {
                        if (ReadFileCNET(file, Correlativo))
                        {
                            if(ReCalcAndSave(ArrayFileName))
                            {
                                Correlativo ++;
                                // Mover archivos
                                MoveFile2Dir(file, "Procesados");
                            } else {
                                oLog.Add("ERROR", String.Format("No logró calcular o grabar XML {0} correctamente.", file));
                                // Mover archivos a Objetados
                                MoveFile2Dir(file, "Objetados");
                            }   
                        } else {
                                oLog.Add("ERROR", String.Format("No pudo leer xml {0} ", file));
                                // Mover archivos a Objetados
                                MoveFile2Dir(file, "Objetados");
                        } 
                                        
                        // Limpia Objs
                        DbCtacte.Clear();
                    } else {
                        oLog.Add("INFO", String.Format("Ignora archivo (no es xml) {0} ", file));
                    }
                    
                }  
                /*
                    // Sólo recopilar Info en Tabla Dinámica
                    foreach (var DbTable in DbTable)
                    {
                        foreach (var DbDetail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero ))
                        {
                             // Sólo Tabla Dinámica, Quitar con refactor
                            DbDetail.Fecha = DbTable.Fecha;
                            DbDetail.Ctacte = DbTable.Ctacte;
                            DbDetail.NombreCliente = DbTable.NombreCliente;
                            DbDetail.CondPago = DbTable.CondPago;
                            DbDetail.CasillaEDI = DbTable.CasillaEDI;
                            DbDetail.TipoPlazoPago = DbTable.TipoPlazoPago;
                            DbDetail.PlazoPago = DbTable.PlazoPago;
                            DbDetail.TipoOC = DbTable.TipoOC;
                            DbDetail.NroInternoProveedor = DbTable.NroInternoProveedor;
                            DbDetail.GLNDireccionDespacho = DbTable.GLNDireccionDespacho;
                            DbDetail.DireccionDespacho = DbTable.DireccionDespacho;
                            DbDetail.FechaVcto = DbTable.FechaVcto;
                            DbDetail.GLNComprador = DbTable.GLNComprador;
                            DbDetail.UniqueId = DbTable.UniqueId;
                            DbDetail.SalesDepartament = DbTable.SalesDepartament;
                            DbDetail.Proceso = DbTable.Proceso;
                        }
                    }
                    
                    oLog.Add("CECHEADER", String.Format("{0} ", ToCsvHeader(DbTable[0])));
                    foreach (var DbTable in DbTable)
                    {
                        var output1 = ToCsvRow(DbTable);
                        oLog.Add("CECHEADER", String.Format("{0} ", output1));
                    }
                    
                    oLog.Add("CECDETALLE", String.Format("{0} ", ToCsvHeader(DbDetail[0])));
                    foreach (var det in DbDetail)
                    {
                        det.Observaciones = ""; 
                        var output1 = ToCsvRow(det);
                        oLog.Add("CECDETALLE", String.Format("{0} ", output1));
                    }
                    */
                }
                catch (Exception ex)
                {
                    oLog.Add("ERROR", ex.Message);
                }



        }
        static bool ReCalcAndSave(string[] ArrayFileName)
        {
            bool ifError = false;  // Flag de control de escritura

            var Dbt = DbTable.Where(x => x.CasillaEDI == ArrayFileName[0].ToString() && x.Numero == ArrayFileName[2].ToString());
            if (Dbt.Count() == 0) return false;

            foreach(var DbTable in DbTable.Where(x => x.CasillaEDI == ArrayFileName[0].ToString() && x.Numero == ArrayFileName[2].ToString()))
            {           
                if (ValidaExistenciaOC(DbTable))
                {
                    oLog.Add("ERROR", String.Format("Documento ya existe en ERP Flexline OC {0}-{1} ", DbTable.Numero, DbTable.NombreCliente));
                    ifError = true;
                }

                int CorrelativoFlexline = GetCorrelativo(DbTable);
                if (CorrelativoFlexline == 0)
                {
                    oLog.Add("INFO", String.Format("No fue posible extraer Max(Correlativo) Gen_Flexline, se usará correlativo Default"));
                } else 
                {
                    DbTable.Correlativo = CorrelativoFlexline;
                }
                
                DataRowCollection RowsGets = GetCodigoProductoFlexline(DbTable);
                if(RowsGets.Count == 0)
                {
                    oLog.Add("INFO", String.Format("No encontró registros en ListaPrecios CNET / Flexline"));
                    ifError = true;
                }

                foreach(DataRow fila in RowsGets)
                {
                    DbTable.Iva = Convert.ToDouble(fila["Iva"]);
                    DbTable.CondPago = fila["CondPagoCnet"].ToString() == "" ? fila["CondPago"].ToString(): fila["CondPagoCnet"].ToString();
                    DbTable.DiasPagoFlexline = fila["CondPagoCnet"].ToString() == "" ? Convert.ToInt32(fila["DiasPago"]) : DbTable.PlazoPago;

                    foreach (var DbDetail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero && x.Item == fila["ItemUpc"].ToString()))
                    {

                        DbDetail.ItemFlexlineLPCNet = fila["ItemFlexline"].ToString() != "" ? fila["ItemFlexline"].ToString() : "No Existe!";
                        DbDetail.GlosaLPCnet = fila["GlosaLPCnet"].ToString() != "" ? fila["GlosaLPCnet"].ToString() : "Producto No Existe en LPCNet o no posee código Flexline";

                        DbDetail.ItemColorLPCNet = fila["ItemColor"].ToString();

                        DbDetail.PrecioLPCNET = Convert.ToDouble(fila["Precio"]);
                        DbDetail.ListaPrecioFlexline = fila["ListaPrecio"].ToString();
                        DbDetail.PrecioFlexline = Math.Round(Convert.ToDouble(fila["PrecioLPFlexline"]));
                        DbDetail.FechaVigencialp = Convert.ToDateTime(fila["FechaVigencia"].ToString());
                        DbDetail.FechaInicio = Convert.ToDateTime(fila["Fec_Inicio"].ToString());
                        DbDetail.FechaFin = Convert.ToDateTime(fila["Fec_Final"].ToString());
                        DbDetail.UnidadFlexline = fila["Unidad"].ToString();
                        DbDetail.VigenciaProductoFlexline = fila["Vigente"].ToString();

                        DbDetail.UnidadContenedoraLPCNET = Convert.ToDouble(fila["Factor"]);

                        // Calcula desde Precio Total (Obsoleto 02.11.2020)
                        //DbDetail.CantidadConvertidaFlexline = DbDetail.Cantidad 
                        //      * (DbDetail.UnidadContenida == 0 || (DbDetail.UnidadFlexline == "KG" || DbDetail.UnidadFlexline == "UN") ? 1 : DbDetail.UnidadContenida) // KG, UN
                        //      * (DbDetail.UnidadContenedoraLPCNET == 0.00 ? 1 : DbDetail.UnidadContenedoraLPCNET);

                        // Precio / por UnidadContenedora
                        DbDetail.PrecioConvertidoFlexline = DbDetail.Precio / (DbDetail.UnidadContenedoraLPCNET == 0 ? 1 : DbDetail.UnidadContenedoraLPCNET);

                        DbDetail.PrecioAjustado = DbDetail.Total / DbDetail.Cantidad;

                        DbDetail.ValDRLineal = DbDetail.PrecioConvertidoFlexline * (DbDetail.DRLineal / 100.00);
                        DbDetail.ValDRLineal2 = (DbDetail.PrecioConvertidoFlexline + DbDetail.ValDRLineal) * (DbDetail.DRLineal2 / 100.00);
                        DbDetail.ValDRLineal3 = (DbDetail.PrecioConvertidoFlexline + DbDetail.ValDRLineal + DbDetail.ValDRLineal2) * (DbDetail.DRLineal3 / 100.00);
                        DbDetail.ValDRLineal4 = (DbDetail.PrecioConvertidoFlexline + DbDetail.ValDRLineal + DbDetail.ValDRLineal2 + DbDetail.ValDRLineal3) * (DbDetail.DRLineal4 / 100.00);
                        DbDetail.ValDRLineal5 = (DbDetail.PrecioConvertidoFlexline + DbDetail.ValDRLineal + DbDetail.ValDRLineal2 + DbDetail.ValDRLineal3 + DbDetail.ValDRLineal4) * (DbDetail.DRLineal5 / 100.00);
                        
                        // Cantidad desde Total línea 
                        double PrecioAjustado = (DbDetail.PrecioConvertidoFlexline 
                                                + DbDetail.ValDRLineal + DbDetail.ValDRLineal2
                                                + DbDetail.ValDRLineal3 + DbDetail.ValDRLineal4  
                                                + DbDetail.ValDRLineal5);

                        DbDetail.CantidadConvertidaFlexline =  DbDetail.Total / (PrecioAjustado == 0? 1: PrecioAjustado);
                        DbDetail.TotalConvertidoFlexline = Math.Round(DbDetail.CantidadConvertidaFlexline * PrecioAjustado);
                    }
                }
                
                // Llena Observaciones y Normaliza Diferencias
                foreach (DocumentoD DbDetail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                {
                    if (String.IsNullOrEmpty(DbDetail.ItemFlexlineLPCNet))  
                    {
                        DbDetail.GlosaLPCnet = DbDetail.Descripcion + " (Producto No Existe en LPCNet o no posee código Flexline)";
                        DbDetail.CantidadConvertidaFlexline = DbDetail.Cantidad;
                        DbDetail.PrecioConvertidoFlexline = DbDetail.PrecioAjustado;
                        DbDetail.TotalConvertidoFlexline += DbDetail.CantidadConvertidaFlexline * DbDetail.PrecioConvertidoFlexline;
                    }

                    DbDetail.Observaciones = ""; 
                    if (String.IsNullOrEmpty(DbDetail.ItemFlexlineLPCNet) )
                    {
                        DbDetail.Observaciones += "- Producto no Existe en LPCNet\n";
                        // Normaliza ItemFlexline sólo si no encuentra
                        DbDetail.ItemFlexlineLPCNet = "No Existe!"; 
                    }
                    else {
                        DbDetail.Observaciones += DbDetail.UnidadContenida == 0? "- Unidad de Empaque no encontrada\n":"";
                        DbDetail.Observaciones += DbDetail.VigenciaProductoFlexline != "S"? "- Producto no Vigente en Flexline \n":"";
                        DbDetail.Observaciones += Math.Abs(DbDetail.PrecioConvertidoFlexline - 
                                                    DbDetail.PrecioFlexline) > (DbDetail.PrecioFlexline * 0.01) 
                                                ? "- Precio distinto a Lista de Precios Flexline (1% Tolerancia)\n"
                                                :"";
                        DbDetail.Observaciones += Math.Abs(DbDetail.TotalConvertidoFlexline - 
                                                    DbDetail.Total) > (DbDetail.Total * 0.01) 
                                                ? String.Format("- Subtotal distinto, ver conversión de cantidades (1% Tolerancia) {0} {1} \n",DbDetail.Total, DbDetail.PrecioConvertidoFlexline)
                                                :"";
                    }

                    // Calculo de Totales
                    DbTable.Total +=  Math.Round(DbDetail.TotalConvertidoFlexline);

                    //Asigna LP (Asume que todas las líneas del docto traen el mismo dato a partir de rev. de archivos 2020)
                    DbTable.ListaPrecioCNET_ItemColor = String.IsNullOrEmpty(DbDetail.ListaPrecioFlexline)?"":DbDetail.ListaPrecioFlexline;
                    DbTable.isLpVencida = (DbTable.Fecha < DbDetail.FechaInicio || DbTable.Fecha > DbDetail.FechaFin);

                    // Sólo Tabla Dinámica, Quitar con refactor
                    /*
                    // DbDetail.Fecha = DbTable.Fecha;
                    // DbDetail.Ctacte = DbTable.Ctacte;
                    // DbDetail.NombreCliente = DbTable.NombreCliente;
                    // DbDetail.CondPago = DbTable.CondPago;
                    // DbDetail.CasillaEDI = DbTable.CasillaEDI;
                    // DbDetail.TipoPlazoPago = DbTable.TipoPlazoPago;
                    // DbDetail.PlazoPago = DbTable.PlazoPago;
                    // DbDetail.TipoOC = DbTable.TipoOC;
                    // DbDetail.NroInternoProveedor = DbTable.NroInternoProveedor;
                    // DbDetail.GLNDireccionDespacho = DbTable.GLNDireccionDespacho;
                    // DbDetail.DireccionDespacho = DbTable.DireccionDespacho;
                    // DbDetail.FechaVcto = DbTable.FechaVcto;
                    // DbDetail.GLNComprador = DbTable.GLNComprador;
                    // DbDetail.UniqueId = DbTable.UniqueId;
                    // DbDetail.SalesDepartament = DbTable.SalesDepartament;
                    // DbDetail.Proceso = DbTable.Proceso;
                    */

                }
                
                var Obser4Aprobacion = DbDetail.Where(x =>  x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero && x.Observaciones != "");
                DbTable.Aprobacion = Obser4Aprobacion.Count() == 0?"S":"P";

                // Si trae Observaciones no graba archivos y mueve a objetados
                ifError = !ifError ? Obser4Aprobacion.Count() != 0: ifError;

                if (!ifError && DbTable.DireccionDespacho != "")
                {
                    DbTable.Proceso = "Procesado";
                    int Resp = Program_Write(DbTable);
                    if (Resp == -1) 
                    {
                        oLog.Add("ERROR", String.Format("Error al intentar grabar la OC {0} de {1}", DbTable.Numero, DbTable.NombreCliente));
                        ifError = true;
                    }
                } else { 
                    ifError = true;
                    DbTable.Proceso = "Objetado";
                    if (String.IsNullOrEmpty(DbTable.DireccionDespacho))
                    {
                        oLog.Add("ERROR", String.Format("Sin Dirección de Despacho", DbTable.Numero, DbTable.NombreCliente));
                    }
                } 

                // Envía Email, aún si no se escribió en tablas GEN
                SendEmail(DbTable, ifError);

            }
            return !ifError;   // Lógica inversa para controlar mov. a Objetados
        }
        static bool ReadFileCNET(string filename, int Correlativo)
        {
            oLog.Add("TRACE", String.Format("Leyendo Archivo {0} ", filename));
            try
            {
                //  El  archivo se ciñe al siguiente formato:
                // <Casilla EDI Emisor>.<Casilla EDI Receptor>.<Nodocumento>.<función>.<año><mes><día><hora><minuto>
                string onlyfileName = filename.Substring(Params.DirectorioFTPCNET.Length + 1).Replace(".xml", "");
                string[] ArrayFileName = onlyfileName.Split(".");

                XElement Xml4LINQ = XElement.Load(filename);  

                var EmpresaFlex= Params.RutSociedades.Find(p => p[3] == ArrayFileName[1]);
                string Empresa = EmpresaFlex == null? "": EmpresaFlex[2];

                DataRowCollection RowsGets = GetCtacteFlexline(Empresa, ArrayFileName);
                if(RowsGets.Count == 0)
                {
                    oLog.Add("INFO", String.Format("No encontró Ctacte en Flexline (ver AnalisisCtacte5), se utilizan datos de Configuración XML."));
                    
                    var CtacteFlex= Params.CtacteComercioNet2Flex.Find(p => p[0] == ArrayFileName[0]);
                    CtacteFlexline xCtacte = new CtacteFlexline();
                    xCtacte.Empresa = Empresa;
                    xCtacte.CasillaEDI = ArrayFileName[0];
                    xCtacte.Ctacte = CtacteFlex == null? "": CtacteFlex[1];
                    xCtacte.RazonSocial =  CtacteFlex == null? "": CtacteFlex[2];
                    DbCtacte.Add(xCtacte);

                } else 
                {
                    foreach(DataRow fila in RowsGets)
                        {
                            CtacteFlexline xCtacte = new CtacteFlexline();
                            xCtacte.Empresa = Empresa;
                            xCtacte.Ctacte = fila["Ctacte"].ToString();
                            xCtacte.RazonSocial = fila["RazonSocial"].ToString();
                            xCtacte.CondPago = fila["CondPago"].ToString();
                            xCtacte.ListaPrecio = fila["ListaPrecio"].ToString();
                            xCtacte.CasillaEDI = fila["CasillaEDI"].ToString();
                            xCtacte.DireccionDespacho = fila["Direccion"].ToString();
                            xCtacte.ComunaDespacho = fila["Comuna"].ToString();
                            xCtacte.CiudadDespacho = fila["Ciudad"].ToString();
                            xCtacte.IdentDireccion = fila["IdentDireccion"].ToString();
                            DbCtacte.Add(xCtacte);
                        }
                }
                // Caso especial
                // =============
                // Hay Archivos que NO traen XNamespace homogeneo en todas las ramas (a diferencia de Wallmart, quién lo hace bien)
                // En estos archivos viene el Namespace sólo en algunas ramas: "transaction" "documentCommand" "order"
                // Se implementa regla para su lectura sólo sobre esas ramas
                // Obs: Si hay otros casos similares, se debe implementar la regla en código!!!!
                
                XNamespace aw = "http://www.uc-council.org/smp/schemas/eanucc"; 
                string EDIWalmart = "925485K200";
                XNamespace awNone = ArrayFileName[0] == EDIWalmart ?"http://www.uc-council.org/smp/schemas/eanucc": XNamespace.None; 

                IEnumerable<Documento> DoctoImportadora;
                DoctoImportadora =
                    from el in Xml4LINQ
                         .Elements(awNone + "body")
                         .Elements(aw + "transaction")
                         .Elements(awNone + "command")
                         .Elements(aw + "documentCommand")
                         .Elements(awNone + "documentCommandOperand")
                         .Elements(aw + "order")
                         select 
                    new Documento {
                        CasillaEDI = ArrayFileName[0],
                        GLNStarfood = ArrayFileName[1],
                        Ctacte = DbCtacte[0].Ctacte,
                        NombreCliente = DbCtacte[0].RazonSocial,
                        Empresa = Empresa,
                        Correlativo = Correlativo,
                        Numero = ArrayFileName[2],
                        Fecha = (DateTime)el.Attribute("creationDate"),
                      
                        TipoPlazoPago = (string)el.Element(awNone + "paymentTerms").Element(awNone + "netPayment")
                                                .Element(awNone + "timePeriodDue").Attribute(awNone + "type"),
                        PlazoPago = (int)el.Element(awNone + "paymentTerms").Element(awNone + "netPayment")
                                            .Element(awNone + "timePeriodDue"),

                        FechaVcto = (DateTime)el.Element(awNone + "movementDate"),
                        UniqueId = (string)el.Element(awNone + "typedEntityIdentification").Element(awNone + "entityIdentification")
                                            .Element(awNone + "uniqueCreatorIdentification"),

                        SalesDepartament = (string)el.Element(awNone + "salesDepartamentNumber"),
                        TipoOC = (string)el.Element(awNone + "orderType"),
                        Promocion = (string)el.Element(awNone + "promotionDealNumber"),
                        NroInternoProveedor = (string)el.Element(awNone + "internalVendorNumber"),
                        GLNComprador = (string)el.Element(awNone + "buyer").Element(awNone + "gln"),
                        GLNDireccionDespacho = (string)el.Element(awNone + "shipParty").Element(awNone + "gln"),
                        ArchivoCNet = filename

                    };
                    foreach (var Encabezado in DoctoImportadora)  
                    {                        
                        oLog.Add("TRACE", String.Format("Procesa Orden {0} {1} del {2}", Encabezado.Numero, Encabezado.NombreCliente, Encabezado.Fecha));
                        if(RowsGets.Count == 0)
                        {
                            var DirDespacho= Params.GLNlocacion.Find(p => p.GLNDespacho == Encabezado.GLNDireccionDespacho);
                            Encabezado.DireccionDespacho = DirDespacho == null? "": DirDespacho.Direccion + " " + DirDespacho.Comuna;

                            foreach(var DbCtacte in DbCtacte.Where(x => x.Empresa == Empresa && x.Ctacte == Encabezado.Ctacte ))
                            {
                                DbCtacte.IdentDireccion = Encabezado.GLNDireccionDespacho;
                                DbCtacte.DireccionDespacho =  Encabezado.DireccionDespacho;
                                DbCtacte.ComunaDespacho = DirDespacho == null? "": DirDespacho.Comuna;
                                DbCtacte.CiudadDespacho = DirDespacho == null? "": DirDespacho.Comuna;
                                break;
                            }

                        } else {
                            // Uso en SendMail
                            var DirDespacho= DbCtacte.Find(p => p.IdentDireccion == Encabezado.GLNDireccionDespacho);
                            Encabezado.DireccionDespacho = DirDespacho == null? "": DirDespacho.DireccionDespacho + " " + DirDespacho.ComunaDespacho + " " + DirDespacho.CiudadDespacho;
                        }

                        DbTable.Add(Encabezado);
                        
                    }

                // Líneas de Detalle

                IEnumerable<DocumentoD> DetalleDistribuidora =
                    from el in Xml4LINQ
                        .Elements(awNone + "body")
                        .Elements(aw + "transaction")
                        .Elements(awNone + "command")
                        .Elements(aw + "documentCommand")
                        .Elements(awNone + "documentCommandOperand")
                        .Elements(aw + "order")
                        .Elements(awNone + "lineItem")
                        let DrsLineal = (from d in el.Elements(awNone + "allowanceCharge") //.Elements(awNone + "monetaryAmountOrPercentage").Elements(awNone + "percentage")
                                         select new 
                                         {
                                             Tipo = (string)d.Attribute("allowanceOrChargeType"),
                                             Porcentaje = ((int)d.Element(awNone + "monetaryAmountOrPercentage").Element(awNone + "percentage")) * ((string)d.Attribute("allowanceOrChargeType") == "CHARGE" ? 1 : -1),
                                         }).ToList()
                    select new DocumentoD {
                        Empresa = Empresa,
                        Numero = ArrayFileName[2],

                        Linea = (int)el.Attribute("number"),
                        Precio = (Double)el.Element(awNone + "netPrice").Element(awNone + "amount"),
                        ListaPrecio = (string)el.Element(awNone + "PriceType"),

                        Cantidad = (Double)el.Element(awNone + "requestedQuantity"),
                        CantidadUnitType = (string)el.Element(awNone + "requestedQuantity").Attribute("UnitType"),

                        UnidadContenida = (Double)el.Element(awNone + "containedUnits"),
                        UnidadContenidaUnitType = (string)el.Element(awNone + "containedUnits").Attribute("UnitType"),

                        DRLineal = DrsLineal.Count  > 0 ? DrsLineal[0].Porcentaje : 0,
                        DRLineal2 = DrsLineal.Count > 1 ? DrsLineal[1].Porcentaje : 0,
                        DRLineal3 = DrsLineal.Count > 2 ? DrsLineal[2].Porcentaje : 0,
                        DRLineal4 = DrsLineal.Count > 3 ? DrsLineal[3].Porcentaje : 0,
                        DRLineal5 = DrsLineal.Count > 4 ? DrsLineal[4].Porcentaje : 0,

                        //DRLinealNodes = (from d in el.Descendants(awNone + "allowanceCharge").Nodes() select d).ToList(),

                        //DRLinealnumerable = (from d in el.Elements(awNone + "allowanceCharge") select d), 


                        Item = (string)el.Element(awNone + "itemIdentification").Element(awNone + "gtin"),
                        ItemBuyer = (string)el.Element(awNone + "itemIdentification").Element(awNone + "buyerItemNumber"),
                        ItemVendor = (string)el.Element(awNone + "itemIdentification").Element(awNone + "vendorItemNumber"),

                        ItemColor = (string)el.Element(awNone + "itemColor"),
                        ItemSize = (string)el.Element(awNone + "itemSize"),
                        Descripcion = (string)el.Element(awNone + "itemDescription").Element(awNone + "text"),

                        Total = (Double)el.Element(awNone + "totalAmount").Element(awNone + "amount")


                    };

                foreach (DocumentoD Registro in DetalleDistribuidora)  
                {                  

                    DbDetail.Add(Registro);   
                }
            
                oLog.Add("TRACE", String.Format("Lectura xml con éxito", ""));
                return true;
                }

            catch (Exception ex)
            {
                oLog.Add("ERROR",
                    String.Format("Error al obtener OC's {0}", ex.Message));
                return false;
            }

        }
        public static void SendEmail(Documento DbTable, bool ifError) 
        {
            try 
            {
                MailMessage Mensaje = new MailMessage();
                Mensaje.To.Add(new MailAddress(Params.EmailTO));
                Mensaje.To.Add(new MailAddress(Params.EmailTO2));
                Mensaje.From = new MailAddress(Params.EmailUser);
        
                Mensaje.IsBodyHtml = true;

                Mensaje.Subject = String.Format("O/Compra {0} ComercioNet {1} - {2}", ifError?"CON ERROR":"",DbTable.Numero, DbTable.NombreCliente);

                SmtpClient smtp = new SmtpClient();
                NetworkCredential credencial = new NetworkCredential()
                {
                    UserName = Params.EmailUser,
                    Password = Params.EmailPassword,
                };

                smtp.UseDefaultCredentials = false;
                smtp.Credentials = credencial;
                smtp.Host = Params.SMTPName;
                smtp.Port = Params.SMTPPort;
                smtp.EnableSsl = Params.EnableSSL;

                string Texto = "<p>Estimado Usuario,</p>";
                Texto += "<p>A Continuaci&oacute;n se presenta el detalle de la Orden de Compra de la empresa {0} por un total del ${1} registrada en ComercioNet.&nbsp;</p>";

                string Normal = "<p><span style='color: #008000;'>El Documento no presenta diferencias y est&aacute; listo para su proceso de Integraci&oacute;n con ERP Flexline, favor proceder.</span></p>";
                string Warning =    "<p><span style='color: #ff0000;'>*** El Documento está listo para su proceso de integraci&oacute;n, pero posee diferencias (quedar&aacute; con estado pendiente).&nbsp; Favor revisar y proceder. ***</span></p>";
                string ErrorEmail = "<p><span style='color: #ff0000;'>*** El Documento NO PUDO SER INGRESADO. Favor revisar y contactar al Administrador si procede. ***</span></p>";

                string EncabezadoPrincipal = "";
                EncabezadoPrincipal += "<table style='border-collapse:collapse;border-spacing:0' class='tg'>";
                EncabezadoPrincipal += "<tbody>";
                EncabezadoPrincipal += "<tr>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:bold;text-decoration:none'>Emisor:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'>{0}</td>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:bold;text-decoration:none'>Receptor:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'>{1}</td>";
                EncabezadoPrincipal += "</tr>";
                EncabezadoPrincipal += "<tr>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Número O/C:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{2}</span></td>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Fecha generación Mensaje:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{3}</span></td>";
                EncabezadoPrincipal += "</tr>";
                EncabezadoPrincipal += "<tr>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Fecha de Embarque:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{4}</span></td>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Fecha de Cancelacion:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{5}</span></td>";
                EncabezadoPrincipal += "</tr>";
                EncabezadoPrincipal += "<tr>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Condiciones de Pago:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{6}</span></td>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Lugar de Entrega:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{7}</span></td>";
                EncabezadoPrincipal += "</tr>";

                EncabezadoPrincipal += "<tr>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Lista Precios ERP:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{8}</span></td>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>CondPago ERP:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{9}</span></td>";
                EncabezadoPrincipal += "</tr>";
                EncabezadoPrincipal += "<tr>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Promoción:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{10}</span></td>";
                EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Num. Proveedor:</span></td>";
                EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{11}</span></td>";
                EncabezadoPrincipal += "</tr>";

                EncabezadoPrincipal += "</tbody>";
                EncabezadoPrincipal += "</table>";
        
                string DetalleHead = "";    
                DetalleHead += "<table style='border-collapse:collapse;border-spacing:0' class='tg'>";
                DetalleHead += "    <thead>";
                DetalleHead += "        <tr>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Linea</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:bold;text-decoration:none'>Cod. UPC</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>ITEM-Flexline</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Descripción</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Talla/UM</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Color/Desc</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:bold;text-decoration:none'>Cantidad</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Precio Flexline</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Precio Unit.</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>D/R</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Unid/Emp.</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Empaques</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:bold'>Total</span></th>";
                DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:bold'>Observaciones</span></th>";
                DetalleHead += "        </tr>";
                DetalleHead += "    </thead>";
                DetalleHead += "    <tbody>";

                string DetalleItem = "";    
                DetalleItem += "        <tr>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'>{0}</td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{1}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{2}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{3}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{4}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{5}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{6}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{7}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{8}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{9}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:400;font-style:normal'>{10}</span></td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'>{11}</td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'>{12}</td>";
                DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:left;vertical-align:top;word-break:normal;color:red'>{13}</td>";
                DetalleItem += "        </tr>";

                string footerTabla = @"
                        <td style='color: #ffffff; font-family: Arial, sans-serif; font-size: 10px;'>
                        &reg; Powered by Codevsys 2020 para Starfood <br/>
                        </td>";

                string EmpresaStarfood;
            
                var NomStarfood = Params.RutSociedades.Find(z => z[3] == DbTable.GLNStarfood);
                EmpresaStarfood = NomStarfood == null? "Starfood": NomStarfood[1];
            
                EncabezadoPrincipal = String.Format(EncabezadoPrincipal, 
                                    DbTable.NombreCliente, EmpresaStarfood, DbTable.Numero, DbTable.Fecha.ToString("dd-MM-yyyy"), 
                                    DbTable.Fecha.ToString("dd-MM-yyyy"), DbTable.FechaVcto.ToString("dd-MM-yyyy"), DbTable.PlazoPago + " " + DbTable.TipoPlazoPago,
                                    DbTable.DireccionDespacho == ""? "Falta Dir. Despacho": DbTable.DireccionDespacho,
                                    (DbTable.ListaPrecioCNET_ItemColor + (DbTable.isLpVencida?" **Vencida**":"")), DbTable.CondPago, 
                                    DbTable.Promocion, DbTable.NroInternoProveedor);

                string DetalleItemDocumento = "";
                string Obs = "";
            
                foreach (DocumentoD Detail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                {                
                    Obs += Detail.Observaciones;

                    DetalleItemDocumento += String.Format(DetalleItem,
                        Detail.Linea, Detail.Item, Detail.ItemFlexlineLPCNet, Detail.GlosaLPCnet, Detail.ItemSize,
                        Detail.ItemColor, Detail.CantidadConvertidaFlexline, Detail.PrecioFlexline.ToString("#,##0"), Detail.PrecioConvertidoFlexline.ToString("#,##0"), (Detail.ValDRLineal + Detail.ValDRLineal2 + Detail.ValDRLineal3 + Detail.ValDRLineal4 + Detail.ValDRLineal5).ToString("#,##0") , 
                        Detail.UnidadContenida,
                        Detail.Cantidad.ToString("#,##0.00") , Detail.TotalConvertidoFlexline.ToString("#,##0"), Detail.Observaciones.Replace("\n","<br>")
                        );
                
                }

                DetalleItemDocumento += "        </tbody>";
                DetalleItemDocumento += "</table>";

                Texto = String.Format(Texto, 
                        DbTable.NombreCliente, Math.Round(DbTable.Total).ToString("#,##0"));

                Mensaje.Body = Texto + (Obs == "" && !DbTable.isLpVencida? Normal : !ifError ? Warning: ErrorEmail) 
                             +"<p>&nbsp;</p>" + EncabezadoPrincipal + "<p>&nbsp;</p>" 
                             + DetalleHead + DetalleItemDocumento 
                             + footerTabla;
                
                smtp.Send(Mensaje);


                oLog.Add("INFO", String.Format("Email enviado con OC {0}", DbTable.Numero));

            }
            catch {
                   oLog.Add("ERROR", String.Format("No puedo enviar Email con OC {0}", DbTable.Numero));

            }

        }
        static public int Program_Write(Documento DbTable)
            {
            
                var Ctacte = DbCtacte.Find(x => x.Empresa == DbTable.Empresa && x.Ctacte == DbTable.Ctacte && x.IdentDireccion == DbTable.GLNDireccionDespacho );

                using (SqlConnection connection = new SqlConnection(Params.StringConexionFlexline))
                {
                    string Sqltext = "";
                    connection.Open();

                    SqlCommand command = connection.CreateCommand();
                    SqlTransaction transaction;
                    transaction = connection.BeginTransaction("CNET2Flexline");

                    command.Connection = connection;
                    command.Transaction = transaction;
                    try
                        {
                            command.CommandType = CommandType.Text;
                            command.CommandTimeout = 30;
                            // Documento
                            
                            Sqltext = "INSERT INTO GEN_DOCUMENTO (" 
                            + " Empresa, TipoDocto, Correlativo, CtaCte, Numero, Fecha, Proveedor, Cliente, Bodega, Bodega2, "
                            + " Local, Comprador, Vendedor, CentroCosto, FechaVcto, ListaPrecio, Analisis, Zona, TipoCta, Moneda, "
                            + " Paridad, RefTipoDocto, RefCorrelativo, ReferenciaExterna, Neto, SubTotal, Total, NetoIngreso, SubTotalIngreso, TotalIngreso, "
                            + " Centraliza, Valoriza, Costeo, Aprobacion, TipoComprobante, NroComprobante, FechaComprobante, PeriodoLibro, FactorMonto, FactorMontoProyectado, "
                            + " TipoCtaCte, IdCtaCte, Glosa, Comentario1, Comentario2, Comentario3, Comentario4, Estado, FechaEstado, NroMensaje, "
                            + " Vigencia, Emitido, PorcentajeAsignado, Adjuntos, Direccion, Ciudad, Comuna, EstadoDir, Pais, Contacto, "
                            + " FechaModif, FechaUModif, UsuarioModif, ComisionCantidad, ComisionLPrecio, ComisionMonto, Hora, Caja, Pago, Donacion, "
                            + " IdApertura, DrCondPago, PorcDr1, PorcDr2, PorcDr3, PorcDr4, Multipagina, NetoBimoneda, SubtotalBimoneda, TotalBimoneda, "
                            + " ParidadBimoneda, ParidadAdic, AnalisisE1, AnalisisE2, AnalisisE3, AnalisisE4, UsuarioAprueba, ANALISISE5, ANALISISE6, ANALISISE7, "
                            + " ANALISISE8, ANALISISE9, ANALISISE10, ANALISISE11, ANALISISE12, ANALISISE13, ANALISISE14, ANALISISE15, ANALISISE16, ANALISISE17, "
                            + " ANALISISE18, ANALISISE19, ANALISISE20, IdFolioSucursal, SUPER_DR, usuariocierre, FechaCierre, AnalisisE21, AnalisisE22, AnalisisE23, "
                            + " AnalisisE24, AnalisisE25, AnalisisE26, AnalisisE27, AnalisisE28, AnalisisE29, AnalisisE30, FechaAprueba, NroImprimir, Telefono, "
                            + " Fax, Emailc, IdSEG_EMPRESA, IdLISTAPRECIO, IdTIPODOCUMENTO_REF, IdDOCUMENTO_REF, IdCON_ENCCOM, IdTIPODOCUMENTO, Id_CTACTE, BoletaIni,"
                            + " BoletaFin, CantAnulados, Clasificacion, AjusteRedondeo, AjusteCondPagoEfectivo, iddocto, FolioReserva, AnalisisE31, AnalisisE32, AnalisisE33, "
                            + " AnalisisE34, AnalisisE35, AnalisisE36, AnalisisE37, AnalisisE38, AnalisisE39, AnalisisE40, AnalisisE41, AnalisisE42, AnalisisE43, "
                            + " AnalisisE44, AnalisisE45, AnalisisE46, AnalisisE47, AnalisisE48, AnalisisE49, AnalisisE50, AnalisisE51, AnalisisE52, AnalisisE53, "
                            + " AnalisisE54, AnalisisE55, AnalisisE56, AnalisisE57, AnalisisE58, AnalisisE59, AnalisisE60 ) ";

                            Sqltext += "Select "
                            + " @empresa, @tipodocto, @correlativo, '' ctacte, @numero, @fecha, '' proveedor, @cliente, @bodega, '' bodega2, "
                            + " '' local, '' comprador, '00 OFICINA' vendedor, '' centrocosto, @fechavcto, @listaprecio, '' analisis, '' zona, '' tipocta, 'PS' moneda, "
                            + " 1 paridad, '' reftipodocto, 0 refcorrelativo, '' referenciaexterna, @neto, @subtotal, @total, @netoingreso, @subtotalingreso, @totalingreso, "
                            + " '' centraliza, 'S' valoriza, '' costeo, @aprobacion, '' tipocomprobante, 0 nrocomprobante, '' fechacomprobante, @periodolibro, 0 factormonto, 1 factormontoproyectado, "
                            + " 'CLIENTE' tipoctacte, @idctacte, @glosa, @comentario1, @comentario2, '' comentario3, '' comentario4, '' estado, '' fechaestado, @nromensaje, "
                            + " 'S' vigencia, 'N' emitido, 0 porcentajeasignado, 'N' adjuntos, @direccion, @ciudad, @comuna, '' estadodir, '' pais, '' contacto, "
                            + " getdate() fechamodif, getdate() fechaumodif, 'CNET2Flexline' usuariomodif, 0 comisioncantidad, 0 comisionlprecio, 0 comisionmonto, @hora, '' caja, 0 pago, 0 donacion, "
                            + " Null idapertura, 0 drcondpago, 0 porcdr1, 0 porcdr2, 0 porcdr3, 0 porcdr4, '' multipagina, @netobimoneda, @subtotalbimoneda, @totalbimoneda, "
                            + " 1 paridadbimoneda, 0 paridadadic, '' analisise1, '' analisise2, '' analisise3, '' analisise4, 'CNET2Flexline' usuarioaprueba, @analisise5, @analisise6, '' analisise7, "
                            + " '' analisise8, '' analisise9, '' analisise10, '' analisise11, '' analisise12, '' analisise13, '' analisise14, '' analisise15, '' analisise16, '' analisise17, "
                            + " '' analisise18, '' analisise19, '' analisise20, '' idfoliosucursal, '' super_dr, '' usuariocierre, '' fechacierre, '' analisise21, '' analisise22, '' analisise23, "
                            + " '' analisise24, '' analisise25, '' analisise26, '' analisise27, '' analisise28, '' analisise29, '' analisise30, Getdate() fechaaprueba, 0 nroimprimir, '' telefono, "
                            + " '' fax, '' emailc, Null idseg_empresa, Null idlistaprecio, Null idtipodocumento_ref, Null iddocumento_ref, Null idcon_enccom, Null idtipodocumento, Null id_ctacte, Null boletaini, "
                            + " Null boletafin, Null cantanulados, Null clasificacion, Null ajusteredondeo, Null ajustecondpagoefectivo, Null iddocto, Null folioreserva, Null analisise31, Null analisise32, Null analisise33, "
                            + " '' analisise34, '' analisise35, '' analisise36, '' analisise37, '' analisise38, '' analisise39, '' analisise40, '' analisise41, '' analisise42, '' analisise43, "
                            + " '' analisise44, '' analisise45, '' analisise46, '' analisise47, '' analisise48, '' analisise49, '' analisise50, '' analisise51, '' analisise52, '' analisise53, "
                            + " '' analisise54, '' analisise55, '' analisise56, '' analisise57, '' analisise58, '' analisise59, '' analisise60 ";

                            command.CommandText = Sqltext;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@empresa",  DbTable.Empresa);
                            command.Parameters.AddWithValue("@tipodocto", "NOTA VENTA CNET");
                            command.Parameters.AddWithValue("@numero", DbTable.Numero);   
                            command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);
                            command.Parameters.AddWithValue("@fecha", DbTable.Fecha);
                            command.Parameters.AddWithValue("@cliente", Ctacte.Ctacte);
                            command.Parameters.AddWithValue("@bodega", DbTable.Empresa == "002"? "01-LIBERTADORES":"01 LAG SUR");
                            command.Parameters.AddWithValue("@fechavcto", DbTable.Fecha.AddDays(DbTable.DiasPagoFlexline != 0? DbTable.DiasPagoFlexline: DbTable.PlazoPago));   
                            command.Parameters.AddWithValue("@listaprecio",  DbTable.ListaPrecioCNET_ItemColor); 

                            command.Parameters.AddWithValue("@neto", Math.Round(DbTable.Total));   
                            command.Parameters.AddWithValue("@subtotal", Math.Round(DbTable.Total));   
                            command.Parameters.AddWithValue("@total", Math.Round(DbTable.Total * (DbTable.Iva/100+1)));   
                            command.Parameters.AddWithValue("@netoingreso", Math.Round(DbTable.Total)); 
                            command.Parameters.AddWithValue("@subtotalingreso", Math.Round(DbTable.Total)); 
                            command.Parameters.AddWithValue("@totalingreso", Math.Round(DbTable.Total * (DbTable.Iva/100+1))); 
                            command.Parameters.AddWithValue("@periodolibro", DbTable.Fecha.Year*100 + DbTable.Fecha.Month);   

                            command.Parameters.AddWithValue("@aprobacion", DbTable.Aprobacion);  
                            command.Parameters.AddWithValue("@idctacte", DbTable.Ctacte);
                            command.Parameters.AddWithValue("@glosa", "" );   
                            command.Parameters.AddWithValue("@comentario1", String.Format("OC:{0};FechaVcto:{1}",DbTable.Numero, DbTable.Fecha.AddDays(DbTable.DiasPagoFlexline != 0? DbTable.DiasPagoFlexline: DbTable.PlazoPago)) );  
                            command.Parameters.AddWithValue("@comentario2", DbTable.ArchivoCNet); 
                            command.Parameters.AddWithValue("@nromensaje", 0);   // TODO: Documentación Flexline dice uso futuro, Corresponde al correlativo interno de compras

                            command.Parameters.AddWithValue("@direccion", Ctacte.DireccionDespacho);   
                            command.Parameters.AddWithValue("@ciudad", Ctacte.CiudadDespacho);   
                            command.Parameters.AddWithValue("@comuna", Ctacte.ComunaDespacho);  

                            command.Parameters.AddWithValue("@hora", DateTime.Now.GetDateTimeFormats('t')[0]); 

                            command.Parameters.AddWithValue("@netobimoneda", Math.Round(DbTable.Total));   
                            command.Parameters.AddWithValue("@subtotalbimoneda", Math.Round(DbTable.Total));   
                            command.Parameters.AddWithValue("@totalbimoneda", Math.Round(DbTable.Total * (DbTable.Iva/100+1)));   

                            command.Parameters.AddWithValue("@analisise5", DbTable.Numero);
                            command.Parameters.AddWithValue("@analisise6", DbTable.Fecha.ToShortDateString());   

                            int Rows = command.ExecuteNonQuery();  
                            if (Rows != -1) {
                                    oLog.Add("INFO", String.Format("Escritura Encabezado OC {0} de {1} completada con Éxito", DbTable.Numero, DbTable.NombreCliente));
                                    }
                            
                            // Documentod
                            Sqltext = "INSERT INTO GEN_DOCUMENTOD ( "
                                + " Empresa, TipoDocto, Correlativo, Secuencia, Linea, Producto, Cantidad, Precio, PorcentajeDR, SubTotal, "
                                + " Impuesto, Neto, DRGlobal, Costo, Total, PrecioAjustado, UnidadIngreso, CantidadIngreso, PrecioIngreso, SubTotalIngreso, "
                                + " ImpuestoIngreso, NetoIngreso, DRGlobalIngreso, TotalIngreso, Serie, Lote, FechaVcto, TipoDoctoOrigen, CorrelativoOrigen, SecuenciaOrigen, "
                                + " Bodega, CentroCosto, Proceso, FactorInventario, FactorInvProyectado, FechaEntrega, CantidadAsignada, Fecha, Nivel, SecciaProceso, "
                                + " Comentario, Vigente, FechaModif, AUX_VALOR1, AUX_VALOR2, AUX_VALOR3, AUX_VALOR4, AUX_VALOR5, AUX_VALOR6, AUX_VALOR7, "
                                + " AUX_VALOR8, AUX_VALOR9, AUX_VALOR10, AUX_VALOR11, AUX_VALOR12, AUX_VALOR13, AUX_VALOR14, AUX_VALOR15, AUX_VALOR16, AUX_VALOR17, "
                                + " AUX_VALOR18, AUX_VALOR19, AUX_VALOR20, VALOR1, VALOR2, VALOR3, VALOR4, VALOR5, VALOR6, VALOR7, "
                                + " VALOR8, VALOR9, VALOR10, VALOR11, VALOR12, VALOR13, VALOR14, VALOR15, VALOR16, VALOR17, "
                                + " VALOR18, VALOR19, VALOR20, CUP, Ubicacion, Ubicacion2, Cuenta, RFGrupo1, RFGrupo2, RFGrupo3, "
                                + " Estado_Prod, Placa, Transportista, TipoPallet, TipoCaja, FactorImpto, SeriePrint, PrecioBimoneda, SubtotalBimoneda, ImpuestoBimoneda, "
                                + " NetoBimoneda, DrGlobalBimoneda, TotalBimoneda, PrecioListaP, Analisis1, Analisis2, Analisis3, Analisis4, Analisis5, Analisis6, "
                                + " Analisis7, Analisis8, Analisis9, Analisis10, Analisis11, Analisis12, Analisis13, Analisis14, Analisis15, Analisis16, "
                                + " Analisis17, Analisis18, Analisis19, Analisis20, UniMedDynamic, ProdAlias, FechaVigenciaLp, LoteDestino, SerieDestino, DoctoOrigenVal, "
                                + " DRGlobal1, DRGlobal2, DRGlobal3, DRGlobal4, DRGlobal5, DRGlobal1Ingreso, DRGlobal2Ingreso, DRGlobal3Ingreso, DRGlobal4Ingreso, DRGlobal5Ingreso, "
                                + " DRGlobal1Bimoneda, DRGlobal2Bimoneda, DRGlobal3Bimoneda, DRGlobal4Bimoneda, DRGlobal5Bimoneda, PorcentajeDr2, PorcentajeDr3, PorcentajeDr4, PorcentajeDr5, ValPorcentajeDr1, "
                                + " ValPorcentajeDr2, ValPorcentajeDr3, ValPorcentajeDr4, ValPorcentajeDr5, ValPorcentajeDr1Ingreso, ValPorcentajeDr2Ingreso, ValPorcentajeDr3Ingreso, ValPorcentajeDr4Ingreso, ValPorcentajeDr5Ingreso, ValPorcentajeDr1Bimoneda, "
                                + " ValPorcentajeDr2Bimoneda, ValPorcentajeDr3Bimoneda, ValPorcentajeDr4Bimoneda, ValPorcentajeDr5Bimoneda, CostoBimoneda, CupBimoneda, MontoAsignado, Analisis21, Analisis22, Analisis23, "
                                + " Analisis24, Analisis25, Analisis26, Analisis27, Analisis28, Analisis29, Analisis30, Receta, CuotaContrato, SecuenciaKit, "
                                + " idDocto, MontoAsignadoIngreso, OrigenIngreso, OrigenCodigo, TOTALRECAR, DESCPRORRA, IdSEG_EMPRESA, IdTIPODOCUMENTO, IdPRODUCTO, iddoctoDet )";

                            String SqltextDet = "Select "
                                + " @empresa, @tipodocto, @correlativo, @secuencia, @linea, @producto, @cantidad, @precio, @porcentajedr, @subtotal, "
                                + " 0 impuesto, @neto, 0 drglobal, 0 costo, @total, @precioajustado, @unidadingreso, @cantidadingreso, @precioingreso, @subtotalingreso, "
                                + " 0 impuestoingreso, @netoingreso, 0 drglobalingreso, @totalingreso, '' serie, '' lote, '' fechavcto, '' tipodoctoorigen, 0 correlativoorigen, 0 secuenciaorigen, "
                                + " @bodega, '' centrocosto, '' proceso, 0 factorinventario, -1 factorinvproyectado, @fechaentrega, 0 cantidadasignada, @fecha, 0 nivel, 0 secciaproceso, "
                                + " @comentario, @vigente, Getdate() fechamodif, '' aux_valor1, '' aux_valor2, '' aux_valor3, '' aux_valor4, '' aux_valor5, '' aux_valor6, '' aux_valor7, "
                                + " '' aux_valor8, '' aux_valor9, '' aux_valor10, '' aux_valor11, '' aux_valor12, '' aux_valor13, '' aux_valor14, '' aux_valor15, '' aux_valor16, '' aux_valor17, "
                                + " '' aux_valor18, '' aux_valor19, '' aux_valor20, 0 valor1, 0 valor2, 0 valor3, 0 valor4, 0 valor5, 0 valor6, 0 valor7, "
                                + " 0 valor8, 0 valor9, 0 valor10, 0 valor11, 0 valor12, 0 valor13, 0 valor14, 0 valor15, 0 valor16, 0 valor17, "
                                + " 0 valor18, 0 valor19, 0 valor20, 0 cup, 'PRINCIPAL' ubicacion, 'PRINCIPAL' ubicacion2, '' cuenta, '' rfgrupo1, '' rfgrupo2, '' rfgrupo3, "
                                + " '' estado_prod, '' placa, '' transportista, '' tipopallet, '' tipocaja, 1 factorimpto, '' serieprint, @preciobimoneda, @subtotalbimoneda, 0 impuestobimoneda, "
                                + " @netobimoneda, 0 drglobalbimoneda, @totalbimoneda, @preciolistap, '' analisis1, '' analisis2, '' analisis3, '' analisis4, '' analisis5, '' analisis6, "
                                + " '' analisis7, '' analisis8, '' analisis9, '' analisis10, '' analisis11, '' analisis12, '' analisis13, '' analisis14, '' analisis15, '' analisis16, "
                                + " '' analisis17, '' analisis18, '' analisis19, '' analisis20, @unimeddynamic, '' prodalias, @fechavigencialp, '' lotedestino, '' seriedestino, 'N' doctoorigenval, "
                                + " 0 drglobal1, 0 drglobal2, 0 drglobal3, 0 drglobal4, 0 drglobal5, 0 drglobal1ingreso, 0 drglobal2ingreso, 0 drglobal3ingreso, 0 drglobal4ingreso, 0 drglobal5ingreso, "
                                + " 0 drglobal1bimoneda, 0 drglobal2bimoneda, 0 drglobal3bimoneda, 0 drglobal4bimoneda, 0 drglobal5bimoneda, @porcentajedr2, @porcentajedr3, @porcentajedr4, @porcentajedr5, @valporcentajedr1, "
                                + " @valporcentajedr2, @valporcentajedr3, @valporcentajedr4, @valporcentajedr5, @valporcentajedr1ingreso, @valporcentajedr2ingreso, @valporcentajedr3ingreso, @valporcentajedr4ingreso, @valporcentajedr5ingreso, @valporcentajedr1bimoneda, "
                                + " @valporcentajedr2bimoneda, @valporcentajedr3bimoneda, @valporcentajedr4bimoneda, @valporcentajedr5bimoneda, 0 costobimoneda, 0 cupbimoneda, 0 montoasignado, '' analisis21, '' analisis22, '' analisis23, "
                                + " '' analisis24, '' analisis25, '' analisis26, '' analisis27, '' analisis28, '' analisis29, '' analisis30, '' receta, 0 cuotacontrato, 0 secuenciakit, "
                                + " 0 iddocto, 0 montoasignadoingreso, 'Producto' origeningreso, @origencodigo, 0 totalrecar, 0 descprorra, 0 idseg_empresa, 0 idtipodocumento, 0 idproducto, 0 iddoctodet ";

                            command.CommandText = Sqltext + " " + SqltextDet;    
                            int Linea = 1;
                            foreach (DocumentoD DbDetail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                            {
                                
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@empresa",  DbTable.Empresa);
                                command.Parameters.AddWithValue("@tipodocto", "NOTA VENTA CNET");
                                command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);
                                command.Parameters.AddWithValue("@secuencia", Linea);
                                command.Parameters.AddWithValue("@linea", Linea);
                                command.Parameters.AddWithValue("@producto", DbDetail.ItemFlexlineLPCNet);
                                command.Parameters.AddWithValue("@cantidad", DbDetail.CantidadConvertidaFlexline);
                                command.Parameters.AddWithValue("@precio", DbDetail.PrecioConvertidoFlexline);
                                command.Parameters.AddWithValue("@subtotal", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@neto", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@total", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@precioajustado", DbDetail.PrecioConvertidoFlexline);

                                command.Parameters.AddWithValue("@porcentajedr", DbDetail.DRLineal);
                                command.Parameters.AddWithValue("@porcentajedr2", DbDetail.DRLineal2);
                                command.Parameters.AddWithValue("@porcentajedr3", DbDetail.DRLineal3);
                                command.Parameters.AddWithValue("@porcentajedr4", DbDetail.DRLineal4);
                                command.Parameters.AddWithValue("@porcentajedr5", DbDetail.DRLineal5);

                                command.Parameters.AddWithValue("@valporcentajedr1", DbDetail.ValDRLineal);
                                command.Parameters.AddWithValue("@valporcentajedr2", DbDetail.ValDRLineal2);
                                command.Parameters.AddWithValue("@valporcentajedr3", DbDetail.ValDRLineal3);
                                command.Parameters.AddWithValue("@valporcentajedr4", DbDetail.ValDRLineal4);
                                command.Parameters.AddWithValue("@valporcentajedr5", DbDetail.ValDRLineal5);

                                command.Parameters.AddWithValue("@valporcentajedr1ingreso", DbDetail.ValDRLineal);
                                command.Parameters.AddWithValue("@valporcentajedr2ingreso", DbDetail.ValDRLineal2);
                                command.Parameters.AddWithValue("@valporcentajedr3ingreso", DbDetail.ValDRLineal3);
                                command.Parameters.AddWithValue("@valporcentajedr4ingreso", DbDetail.ValDRLineal4);
                                command.Parameters.AddWithValue("@valporcentajedr5ingreso", DbDetail.ValDRLineal5);

                                command.Parameters.AddWithValue("@valporcentajedr1bimoneda", DbDetail.ValDRLineal);
                                command.Parameters.AddWithValue("@valporcentajedr2bimoneda", DbDetail.ValDRLineal2);
                                command.Parameters.AddWithValue("@valporcentajedr3bimoneda", DbDetail.ValDRLineal3);
                                command.Parameters.AddWithValue("@valporcentajedr4bimoneda", DbDetail.ValDRLineal4);
                                command.Parameters.AddWithValue("@valporcentajedr5bimoneda", DbDetail.ValDRLineal5);

                                command.Parameters.AddWithValue("@unidadingreso", String.IsNullOrEmpty(DbDetail.UnidadFlexline) ?"UN":DbDetail.UnidadFlexline);
                                command.Parameters.AddWithValue("@cantidadingreso", DbDetail.CantidadConvertidaFlexline);
                                command.Parameters.AddWithValue("@precioingreso", DbDetail.PrecioConvertidoFlexline);
                                command.Parameters.AddWithValue("@subtotalingreso", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@netoingreso", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@totalingreso", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@bodega", DbTable.Empresa == "002"? "01-LIBERTADORES":"01 LAG SUR");
                                command.Parameters.AddWithValue("@fechaentrega", DbTable.Fecha.AddDays(DbTable.PlazoPago));
                                command.Parameters.AddWithValue("@fecha", DbTable.Fecha);

                                command.Parameters.AddWithValue("@comentario", DbDetail.Observaciones);
                                command.Parameters.AddWithValue("@vigente", "S");  

                                command.Parameters.AddWithValue("@preciobimoneda", DbDetail.PrecioConvertidoFlexline);
                                command.Parameters.AddWithValue("@subtotalbimoneda", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@netobimoneda", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@totalbimoneda", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@preciolistap", DbDetail.PrecioFlexline);
                                command.Parameters.AddWithValue("@fechavigencialp", DbDetail.FechaVigencialp);
                                

                                command.Parameters.AddWithValue("@unimeddynamic", DbDetail.CantidadConvertidaFlexline);
                                command.Parameters.AddWithValue("@origencodigo", DbDetail.ItemFlexlineLPCNet);

                                Rows = command.ExecuteNonQuery();
                                if (Rows != -1) {
                                    oLog.Add("INFO", String.Format("Escritura Detalle Línea {0} de {1} {2} completada con Éxito", Linea, DbTable.Numero, DbTable.NombreCliente));
                                    }

                                Linea ++;
                            }
                            // Documentop
                            Sqltext = "INSERT INTO GEN_DOCUMENTOP (" 
                                + " Empresa, TipoDocto, Correlativo, Linea, CodigoPago, TipoPago, FechaVcto, Monto, MontoIngreso, TipoDoctoPago, "
                                + " NroDoctoPago, Cuenta, MontoBimoneda, AjusteBimoneda, Entidad, NumAutoriza, CuentaPago, FechaVctoTarjeta, PropietarioTarjeta, FechaVctoDocto,"
                                + " RutComprador, RutGirador, MonedaPago, MontoPago, ParidadPago, ValorGenerico, LineaTipo, idDocto, IdSEG_EMPRESA, IdTIPODOCUMENTO, "
                                + " iddoctoPag ) ";
                            Sqltext += "Select "
                                + " @empresa, @tipodocto, @correlativo, 1 linea, @codigopago, 'S' tipopago, @fechavcto, @monto, @montoingreso, @tipodoctopago, "
                                + " @nrodoctopago, '' cuenta, @montobimoneda, 0 ajustebimoneda, '' entidad, '' numautoriza, '' cuentapago, '' fechavctotarjeta, '' propietariotarjeta, @fechavctodocto, "
                                + " '' rutcomprador, '' rutgirador, 'PS' monedapago, @montopago, 1 paridadpago, 0 valorgenerico, 0 lineatipo, 0 iddocto, 0 idseg_empresa, 0 idtipodocumento, "
                                + " 0 iddoctopag ";

                            command.CommandText = Sqltext;
                            command.Parameters.Clear();
                            command.Parameters.AddWithValue("@empresa",  DbTable.Empresa);
                            command.Parameters.AddWithValue("@tipodocto", "NOTA VENTA CNET");
                            command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);

                            command.Parameters.AddWithValue("@codigopago", DbTable.CondPago);
                            command.Parameters.AddWithValue("@fechavcto", DbTable.Fecha.AddDays(DbTable.DiasPagoFlexline != 0? DbTable.DiasPagoFlexline: DbTable.PlazoPago));
                            command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * (DbTable.Iva/100+1)));   
                            command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * (DbTable.Iva/100+1)));   
                            
                            command.Parameters.AddWithValue("@tipodoctopago", "NOTA VENTA CNET");
                            command.Parameters.AddWithValue("@nrodoctopago", DbTable.Numero);
                            command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * (DbTable.Iva/100+1)));   
                            command.Parameters.AddWithValue("@fechavctodocto", DbTable.Fecha.AddDays(DbTable.PlazoPago));

                            command.Parameters.AddWithValue("@montopago", Math.Round(DbTable.Total * (DbTable.Iva/100+1)));   

                            Rows = command.ExecuteNonQuery();
                                if (Rows != -1) {
                                    oLog.Add("INFO", String.Format("Escritura CondPago {0} {1} completada con Éxito", DbTable.Numero, DbTable.NombreCliente));
                                    }
                            // DocumentoP
                            Sqltext = "INSERT INTO GEN_DOCUMENTOV ( "
                                    + " Empresa, TipoDocto, Correlativo, Nombre, Orden, Factor, Monto, MontoIngreso, Ajuste, AjusteIngreso, "
                                    + " Texto, Porcentaje, MontoBimoneda, AjusteBimoneda, idDocto, IdSEG_EMPRESA, IdTIPODOCUMENTO, iddoctoVal ) ";

                            Sqltext += " Select " 
                            + " @empresa, @tipodocto, @correlativo, @nombre, @orden, @factor, @monto, @montoingreso, 0 ajuste, 0 ajusteingreso, "
                            + " '' texto, 0 porcentaje, @montobimoneda, 0 ajustebimoneda, 0 iddocto, 0 idseg_empresa, 0 idtipodocumento, 0 iddoctoval " ;

                            // Formulas 
                            command.CommandText = Sqltext;
                            for (int i = 1; i <= 7; i++)
                            {
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@empresa",  DbTable.Empresa);
                                command.Parameters.AddWithValue("@tipodocto", "NOTA VENTA CNET");
                                command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);
                                command.Parameters.AddWithValue("@orden", i);

                                switch (i)
                                {
                                    case 1:
                                        command.Parameters.AddWithValue("@nombre", "Neto");
                                        command.Parameters.AddWithValue("@factor", 0);
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total));
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total)); 
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total)); 
                                        break;
                                    case 2:
                                        command.Parameters.AddWithValue("@nombre", "Descto");
                                        command.Parameters.AddWithValue("@factor", -1);
                                        command.Parameters.AddWithValue("@monto", 0);
                                        command.Parameters.AddWithValue("@montoingreso", 0);
                                        command.Parameters.AddWithValue("@montobimoneda", 0);
                                        break;
                                    case 3:
                                        command.Parameters.AddWithValue("@nombre", "SubTot");
                                        command.Parameters.AddWithValue("@factor", 0);
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total)); 
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total)); 
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total)); 
                                        break;
                                    case 4:
                                        command.Parameters.AddWithValue("@nombre", "DsctoPorcent");
                                        command.Parameters.AddWithValue("@factor", -1);
                                        command.Parameters.AddWithValue("@monto", 0);
                                        command.Parameters.AddWithValue("@montoingreso", 0);
                                        command.Parameters.AddWithValue("@montobimoneda", 0);
                                        break;
                                    case 5:
                                        command.Parameters.AddWithValue("@nombre", "Recargo");
                                        command.Parameters.AddWithValue("@factor", 1);
                                        command.Parameters.AddWithValue("@monto", 0);
                                        command.Parameters.AddWithValue("@montoingreso", 0);
                                        command.Parameters.AddWithValue("@montobimoneda", 0);
                                        break;
                                    case 6:
                                        command.Parameters.AddWithValue("@nombre", "Afecto");
                                        command.Parameters.AddWithValue("@factor", 0);
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total)); 
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total)); 
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total)); 
                                        break;
                                    default:
                                        command.Parameters.AddWithValue("@nombre", "IVA");
                                        command.Parameters.AddWithValue("@factor", 1);
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * DbTable.Iva/100)); 
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * DbTable.Iva/100)); 
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * DbTable.Iva/100)); 
                                        break;
                                }
                                
                                Rows = command.ExecuteNonQuery();
                                if (Rows != -1) {
                                    oLog.Add("INFO", String.Format("Escritura Formula {0} de OC {1} completada con Éxito", i, DbTable.Numero));
                                    }
                            }
                            
                            transaction.Commit();
                            return Rows;
                        }
                        catch (Exception ex)
                        {
                            oLog.Add("ERROR", String.Format("Commit Exception Type: {0} {1}", ex.GetType(), ex.Message));
                            Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                            Console.WriteLine("  Message: {0}", ex.Message);
                            try
                            {
                                transaction.Rollback();
                                return -1;
                            }
                            catch (Exception ex2)
                            {
                                Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                                Console.WriteLine("  Message: {0}", ex2.Message);
                                oLog.Add("ERROR", String.Format("Rollback Exception Type: {0} {1}", ex2.GetType(), ex2.Message));
                                return -1;
                            }
                         }
                }
            }
        public static DataRowCollection GetCodigoProductoSispal(Documento DbTable)
        {   // Obsoleto 01.10.2020 By Cec
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionSisPal;

                        connection.Open();
                        string SqlText = "";

                        SqlText = "Select Isnull(ItemUpc,'') ItemUpc, Isnull(ItemFlexline,'') Producto, "
                            + " ItemColor, Isnull(Descripcion,'') Glosa, Precio, UnidadContenedora, EDI "
                            + " FROM ListaPrecioCnet a With (Nolock)"
                            + " Where "
                            + " a.Empresa = @Empresa and a.ItemUpc in ({0}) ";

                        string Items = "";
                        foreach (DocumentoD Row in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                        {
                            Items += "'" + Row.Item + "',";
                        }
                        
                        Items = Items != ""? Left(Items, Items.Length - 1): "''";

                        SqlText = String.Format(SqlText, Items);
                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Empresa", DbTable.Empresa);

                        SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
                        DataTable dtCommandSQL = new DataTable();
                        Adapter.Fill(dtCommandSQL);

                        return dtCommandSQL.Rows;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Datos Producto en SisPal {0}", ex.Message));
                return null;
            }
        }
        public static DataRowCollection GetCtacteFlexline(string Empresa, string[] fileArray)
        { 
            //Verificar si existe OC con número
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionFlexline;

                        connection.Open();

                        string SqlText = "Select Isnull(a.Ctacte,'') Ctacte, Isnull(a.RazonSocial,'') RazonSocial, "
                            + " Isnull(a.CondPago,'') CondPago, Isnull(a.ListaPrecio,'') ListaPrecio, Isnull(a.AnalisisCtacte5,'') CasillaEDI, "
                            + " Isnull(Cdir.Direccion,'') Direccion, Isnull(Cdir.Comuna,'') Comuna, Isnull(Cdir.Ciudad,'') Ciudad, "
                            + " Isnull(Cdir.IdentDireccion,'') IdentDireccion"
                            + " FROM Ctacte a With (Nolock) "
                            + " Left Join CtaCteDirecciones Cdir With (Nolock) on Cdir.Empresa=a.Empresa and Cdir.Ctacte=a.Ctacte " //and Cdir.IdentDireccion = @DirDespacho"
                            + " Where "
                            + " a.Empresa = @Empresa and a.AnalisisCtacte5 = @CasillaEDI ";

                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Empresa", Empresa);
                        ComandoSQL.Parameters.AddWithValue("CasillaEDI", fileArray[0].ToString());

                        SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
                        DataTable dtCommandSQL = new DataTable();
                        Adapter.Fill(dtCommandSQL);

                        return dtCommandSQL.Rows;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Datos Ctacte en Flexline {0}", ex.Message));
                return null;
            }
        }
        public static bool ValidaExistenciaOC(Documento DbTable)
        { 

            //Verificar si existe OC con número
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionFlexline;

                        connection.Open();
                        // Doctos CNET busca 48 meses atrás (día server)
                        // Doctos anteriores a CNET, sólo 2 meses (regla definida por Starfood)
                        string SqlText = "Select 1 Row "
                            + " FROM Documento a With (Nolock) "
                            + " Where "
                            + " a.Empresa = @Empresa and a.TipoDocto in (@NV1, @NV2, @NV3) "
                            + " and AnalisisE5 = @Numero "
                            + " and Fecha <= Dateadd(m, Case when tipodocto = @NV3 then -48 else -2 End, Getdate() ) "
                            + " and Vigencia <> 'A' "
                            + " Union " 
                            + " Select 1 Row "
                            + " FROM Gen_Documento a With (Nolock) "
                            + " Where "
                            + " a.Empresa = @Empresa and a.TipoDocto in (@NV1, @NV2, @NV3) "
                            + " and AnalisisE5 = @Numero ";
                            

                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Empresa", DbTable.Empresa);
                        ComandoSQL.Parameters.AddWithValue("NV1", "Nota de Venta");
                        ComandoSQL.Parameters.AddWithValue("NV2", "Nota VTA. S/LISTA");
                        ComandoSQL.Parameters.AddWithValue("NV3", "Nota Venta CNET");
                        ComandoSQL.Parameters.AddWithValue("Numero", DbTable.Numero);
                        
                        SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
                        DataTable dtCommandSQL = new DataTable();
                        Adapter.Fill(dtCommandSQL);

                        return (dtCommandSQL.Rows.Count != 0) ;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Datos Ctacte en Flexline {0}", ex.Message));
                return true;
            }
        }
        public static int GetCorrelativo(Documento DbTable)
        { 
            
            //Verificar si existe OC con número
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionFlexline;

                        connection.Open();

                        string SqlText = "Select isnull(Max(Correlativo),0)+1 Correlativo From ( "
                            + " Select isnull(Max(Correlativo),0) Correlativo "
                            + " FROM Documento a With (Nolock) "
                            + " Where "
                            + " a.Empresa = @Empresa and a.TipoDocto = @NV1 "
                            + " UNION All "
                            + " Select isnull(Max(Correlativo),0) Correlativo "
                            + " FROM Gen_Documento a With (Nolock) "
                            + " Where "
                            + " a.Empresa = @Empresa and a.TipoDocto = @NV1 " 
                            + " ) x " ;    
                            

                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Empresa", DbTable.Empresa);
                        ComandoSQL.Parameters.AddWithValue("NV1", "Nota Venta CNET");
                        
                        SqlDataReader Registro = ComandoSQL.ExecuteReader();

                        if (Registro.Read())
                        {
                            return Convert.ToInt32(Registro["Correlativo"]);
                        } else 
                            return 0 ;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Max(Correlativo) Gen_Documento en Flexline {0}", ex.Message));
                return 0;
            }
        }
        public static DataRowCollection GetCodigoProductoFlexline(Documento DbTable)
        {
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionFlexline;

                        connection.Open();
                         string SqlText = "Select a.ItemUpc, a.ItemFlexline, a.Descripcion GlosaLPCnet, a.Precio, a.Factor, a.ListaPrecio, a.ItemColor, "
                        + " Isnull(Lpd.Valor,0) PrecioLPFlexline, Isnull(Lpd.FechaVigencia,'') FechaVigencia, p.Glosa GlosaFlexline, "
                        + " Isnull(Lp.Fec_Inicio,'') Fec_Inicio, Isnull(Fec_Final,'') Fec_Final, Isnull(p.Unidad,'') Unidad, " 
                        + " Isnull(p.Vigente,'N') Vigente, Isnull(Iva.Valor1, 0) Iva, " 
                        + " Isnull(Cta.CondPago,'') CondPago, Isnull(cp.Valor1,0) DiasPago, Isnull(cpCnet.LP,'') CondPagoCnet " 
                        + " From ListaprecioCnet a With (Nolock) "
                        + " Left join Ctacte cta With (Nolock) on cta.empresa=a.empresa and cta.Ctacte = @Ctacte and cta.Tipoctacte='Cliente' "
                        + " Left Join Producto p With (Nolock) on p.Empresa= a.Empresa and p.Producto=a.ItemFlexline "
                        + " Left Join ListaPrecio lp With (Nolock) on Lp.Empresa=a.Empresa and lp.LisPrecio=a.ListaPrecio "
                        + " Left Join ListaPrecioD Lpd With (Nolock) on Lpd.Empresa=lp.Empresa and lpd.IdLisPrecio=Lp.IdLisPrecio and lpd.Producto=a.ItemFlexline "
                        + " Left Join ( "
                        + " Select Empresa, Max(Codigo) LP From Gen_TabCod With (Nolock) Where Empresa=@Empresa and Tipo ='gen_pagoventas' and Valor1 = @DiasPago "
                        + " and RELACIONTIPO1 = 'Cheque' Group by Empresa "
                        + " ) cpCnet on cpCnet.empresa=a.empresa "
                        + " Left Join Gen_TabCod cp With (Nolock) on cp.empresa=a.empresa and cp.tipo='gen_pagoventas' and cp.codigo=Cta.CondPago "
                        + " Left Join ( "
                        + "       Select a.Empresa, a.Texto, convert(date,a.Descripcion,103) FechaIni, Isnull(b.FechaFin, convert(date,'2099-12-31')) FechaFin, " 
                        + "       a.Valor1 From gen_tabcod a  With (Nolock) "
                        + "       OUTER apply ( "
                        + "           select top 1 dateadd(d,-1,cast(left(b.codigo,8) as date)) FechaFin "
                        + "           from GEN_TABCOD b With (Nolock) "
                        + "           where b.empresa=a.empresa and b.texto=a.texto and a.tipo=b.Tipo "
                        + "           AND convert(datetime, b.descripcion, 103) > convert(datetime, a.descripcion, 103) "
                        + "           order by 1 "
                        + "       ) b "
                        + "       where a.empresa = @Empresa and a.tipo = 'config.param' and a.texto='PARIVA' "
                        + " ) Iva on Iva.Empresa = a.Empresa and @Fecha between FechaIni and FechaFin "
                        + " Where a.Empresa=@Empresa and a.ItemColor = isnull(@ItemColor,'') "
                        + " and a.EDI = @CasillaEDI and a.ItemUpc in ({0}) ";

                        string Items = "";
                        string PreFijoLP = "";
                        foreach (DocumentoD Row in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                        {
                            Items += "'" + Row.Item + "',";
                            PreFijoLP = String.IsNullOrEmpty(Row.ItemColor)?"":Row.ItemColor;
                        }
                        
                        Items = Items != ""? Left(Items, Items.Length - 1): "''";

                        SqlText = String.Format(SqlText, Items);
                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Ctacte", DbTable.Ctacte);
                        ComandoSQL.Parameters.AddWithValue("CasillaEDI", DbTable.CasillaEDI);
                        ComandoSQL.Parameters.AddWithValue("Empresa", DbTable.Empresa);
                        ComandoSQL.Parameters.AddWithValue("Fecha", DbTable.Fecha);

                        ComandoSQL.Parameters.AddWithValue("ItemColor", PreFijoLP);
                        ComandoSQL.Parameters.AddWithValue("DiasPago", DbTable.PlazoPago);

                        SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
                        DataTable dtCommandSQL = new DataTable();
                        Adapter.Fill(dtCommandSQL);


                        return dtCommandSQL.Rows;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Datos Producto en LPCNet {0}", ex.Message));
                return null;
            }
        }
        // Tools
        public static bool isFileXML(string file)
        {
            try
            {
                using (XmlTextReader reader = new XmlTextReader(file))
                    {
                    return reader.Read();
                    }
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static void MoveFile2Dir(string file, string directorio)
        {
            string DirProcessed = Path.Combine(@Params.DirectorioFTPCNET , directorio);
            CreateDirectory(DirProcessed);

            oLog.Add("TRACE",
                    String.Format("Terminado: Moviendo archivo {0} a directorio {1}",
                    file.Substring(@Params.DirectorioFTPCNET.Length + 1), directorio));

            File.Move(file, Path.Combine(DirProcessed, file.Substring(@Params.DirectorioFTPCNET.Length + 1)), true);
        }
        public bool VerifyConnectionSQL(string conn)
        {
            string connetionString = null;
            SqlConnection cnn;
            connetionString = conn;
            cnn = new SqlConnection(connetionString);
            try
            {
                cnn.Open();
                cnn.Close();
                return true;
            }
            catch {
                  return false;
            }
        }
        static string Right( string value, int length)
        {
            if (String.IsNullOrEmpty(value)) return string.Empty;

            return value.Length <= length ? value : value.Substring(value.Length - length);
        }
        static string Left( string value, int length)
        {
            if (String.IsNullOrEmpty(value)) return string.Empty;

            return value.Length <= length ? value : value.Substring(0, length);
        }
        public static string ToCsvHeader( object obj)
       {
           Type type = obj.GetType();
           var properties = type.GetProperties(BindingFlags.DeclaredOnly |
                                          BindingFlags.Public |
                                          BindingFlags.Instance);

           string result = string.Empty;
           Array.ForEach(properties, prop =>
           {
               result += prop.Name + ",";
           });

           return (!string.IsNullOrEmpty(result) ? result.Substring(0, result.Length - 1) : result);
       }
        public static string ToCsvRow( object obj)
       {
           Type type = obj.GetType();
           var properties = type.GetProperties(BindingFlags.DeclaredOnly |
                                          BindingFlags.Public |
                                          BindingFlags.Instance);

           string result = string.Empty;
           Array.ForEach(properties, prop =>
           {
               var value = prop.GetValue(obj, null);
               var propertyType = prop.PropertyType.FullName;
               if (propertyType == "System.String")
               {
                   // wrap value incase of commas
                   value = "\"" + value + "\"";
               }

               result += value + ",";

           });

           return (!string.IsNullOrEmpty(result) ? result.Substring(0, result.Length - 1) : result);
       }
        static void CreateDirectory(string Ruta)
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
                      || ex is IOException
                  )
                {
                    throw new Exception(ex.Message);
                }
            }
        }
        static Settings Params = new Settings();
        static App_log oLog = new App_log(AppDomain.CurrentDomain.BaseDirectory);
        static  List<Documento> DbTable = new List<Documento>();
        static  List<DocumentoD> DbDetail = new List<DocumentoD>();
        static List<CtacteFlexline> DbCtacte = new List<CtacteFlexline>();

    }

    public class GLN {
        public String GLNDespacho { get; set; }
        public String Nombre { get; set; }    
        public String Direccion { get; set; }    
        public String Comuna { get; set; }    
    }
    public class Settings
    {
        public List<String[]> RutSociedades { get; set; }
        public List<GLN> GLNlocacion { get; set; }
        public List<String[]> CtacteComercioNet2Flex { get; set; }
        public Uri FTPServer { get; set; }
        public String FTPPort { get; set; }
        public String FTPUser { get; set; }         
        public String FTPPassword { get; set; }
        public String SMTPName { get; set; }
        public int SMTPPort { get; set; }
        public bool EnableSSL { get; set; }
        public String EmailUser { get; set; }
        public String EmailPassword { get; set; }
        public String EmailTO { get; set; }
        public String EmailTO2 { get; set; }
        public String DirectorioFTPCNET { get; set; }
        public String StringConexionFlexline { get; set; }
        public String StringConexionSisPal { get; set; }
    }
    public class CtacteFlexline {
        public String Empresa { get; set; }
        public String Ctacte { get; set; }
        public String RazonSocial { get; set; }    
        public String CasillaEDI { get; set; }
        public String CondPago { get; set; }    
        public String ListaPrecio { get; set; }    
        public String IdentDireccion { get; set; }     // Key GLNDireccionDespacho
        public String DireccionDespacho { get; set; }    
        public String ComunaDespacho { get; set; }    
        public String CiudadDespacho { get; set; }    
    }
    public class Documento
    {
        public String Empresa { get; set; }     // Posición 2 RutSociedades
        public int Correlativo { get; set; }                  // Correlativo para inyección Flex
        public String Ctacte { get; set; }     // Posición 1 CtacteComercioNet2Flex
        public String NombreCliente { get; set; }     // Posición 2 CtacteComercioNet2Flex
        public String CondPago { get; set; }     // Desde Tabla Ctacte Flexline
        public int DiasPagoFlexline { get; set; }     // Desde Tabla Ctacte Flexline
        public DateTime Fecha { get; set; }           // creationDate
        public String Numero { get; set; }          // Desde Nombre de archivo
        public String CasillaEDI { get; set; }             // Desde Nombre de archivo Casilla EDI
        public String GLNComprador { get; set; }     // //buyer/gln  ** 
        public String GLNStarfood { get; set; }     // Desde Nombre de archivo
        public String TipoPlazoPago { get; set; }   // paymentTerms/netPayment/timePeriodDue/@type
        public int PlazoPago { get; set; }          // paymentTerms/netPayment/timePeriodDue      
        public DateTime FechaVcto { get; set; }       // movementDate
        public String UniqueId { get; set; }        // typedEntityIdentification/entityIdentification/uniqueCreatorIdentification
        public String SalesDepartament { get; set; }    // salesDepartamentNumber
        public String TipoOC { get; set; }              // orderType
        public String Promocion { get; set; }           // promotionDealNumber
        public String NroInternoProveedor { get; set; } // internalVendorNumber
        public String GLNDireccionDespacho { get; set; }   //shipParty/gln
        public String DireccionDespacho { get; set; }   //Desde XML
        public String ComunaDespacho { get; set; }   //Comuna desde CtacteDirecciones Flexline 
        public String CiudadDespacho { get; set; }   //Ciudad desde CtacteDirecciones Flexline 
        public Double Total { get; set; }   //Calculado 
        public string ArchivoCNet { get; set; }    // Nombre Archivo
        public string ListaPrecioCNET_ItemColor { get; set; }    // Lista de Precios desde XML, extrae ItemColor desde 1ra. Linea de detalle (se asume que es única a partir de revisión de archivos 2020)
        public bool isLpVencida { get; set; }
        public string Aprobacion { get; set; }    // Flag calculado para Aprobación del Docto 
        public Double Iva { get; set; }   // desde Config.param, Codigo PARIVA de Flexline
        public string Proceso { get; set; } // Sólo Debug, guarda si docto fue procesado, objetado
    }
    public class DocumentoD
    {
        public String Empresa { get; set; }                  // DbTable.Empresa
        public String Numero { get; set; }                  // DbTable.Numero
        public int Linea { get; set; }                  // lineItem/@number
        public Double Precio { get; set; }                 // PrecioXML / UnidadContenida
        public Double PrecioAjustado { get; set; }                 // Incluye D/R, TotalLinea / Cantidad (redondeado a 2)
        public String ListaPrecio { get; set; }         // PriceType
        public Double Cantidad { get; set; }               // CantidadXML * UnidadContenida
        public String CantidadUnitType { get; set; }    // requestedQuantity/@UnitType
        public Double UnidadContenida { get; set; }    // containedUnits
        public String UnidadContenidaUnitType { get; set; }    // containedUnits/@UnitType
        public String Item { get; set; }                // itemIdentification/gtin
        public int DRLineal { get; set; }               // lineItem/allowanceCharge
        public int DRLineal2 { get; set; }              // lineItem/allowanceCharge
        public int DRLineal3 { get; set; }              // lineItem/allowanceCharge
        public int DRLineal4 { get; set; }              // lineItem/allowanceCharge
        public int DRLineal5 { get; set; }              // lineItem/allowanceCharge
        public Double ValDRLineal { get; set; }              // Calculado
        public Double ValDRLineal2 { get; set; }              // Calculado
        public Double ValDRLineal3 { get; set; }              // Calculado
        public Double ValDRLineal4 { get; set; }              // Calculado
        public Double ValDRLineal5 { get; set; }              // Calculado
        public String ItemBuyer { get; set; }           // itemIdentification/buyerItemNumber
        public String ItemVendor { get; set; }          // itemIdentification/vendorItemNumber
        public String ItemSize { get; set; }            // itemSize
        public String ItemColor { get; set; }            // itemColor Indica LP en caso de clientes con más de una
        public String Descripcion { get; set; }         // itemDescription/text
        public Double Total { get; set; }                  // totalAmount/amount
        public String ItemFlexlineLPCNet { get; set; }        // LPCnet ItemFlexline
        public String ItemColorLPCNet { get; set; }          // Desde LPCnet Item_Color 
        public String GlosaLPCnet { get; set; }            // LpCnet Descripcion
        public Double PrecioLPCNET { get; set; }         // LpCnet Precio
        public Double UnidadContenedoraLPCNET { get; set; }         // Factor conversión 
        public String ListaPrecioFlexline { get; set; }    // Flexline/Ctacte/Listaprecio
        public Double PrecioFlexline { get; set; }         // Flexline/ListaPrecio/Valor
        public DateTime FechaVigencialp { get; set; }         // Flexline/ListaPreciod/FechaVigencia
        public DateTime FechaInicio { get; set; }         // Flexline/ListaPrecio/Fec_Inicio
        public DateTime FechaFin { get; set; }         // Flexline/ListaPrecio/Fec_Final
        public String UnidadFlexline { get; set; }         // Flexline/Producto/Unidad
        public String VigenciaProductoFlexline { get; set; }         // Flexline/Producto/Vigencia
        public Double CantidadConvertidaFlexline { get; set; }         // Cantidad * UnidadContenida
        public Double PrecioConvertidoFlexline { get; set; }         // Precio / UnidadContenida
        public Double TotalConvertidoFlexline { get; set; }         // Sum(CantidadConvertidaFlexline*PrecioConvertidoFlexline)
        public String Observaciones { get; set; }         // Obs de conversión para usuario operador

        // Sólo para tabla Dinámica, Quitar al refactor
        public DateTime Fecha { get; set; }           // creationDate
        public String Ctacte { get; set; }     // Posición 1 CtacteComercioNet2Flex
        public String NombreCliente { get; set; }     // Posición 2 CtacteComercioNet2Flex
        public String CondPago { get; set; }     // Desde Tabla Ctacte Flexline
        public String CasillaEDI { get; set; }             // Desde Nombre de archivo Casilla EDI
        public String TipoPlazoPago { get; set; }   // paymentTerms/netPayment/timePeriodDue/@type
        public int PlazoPago { get; set; }          // paymentTerms/netPayment/timePeriodDue     
        public String TipoOC { get; set; }              // orderType
        public String NroInternoProveedor { get; set; } // internalVendorNumber
        public String GLNDireccionDespacho { get; set; }   //shipParty/gln
        public String DireccionDespacho { get; set; }   //Desde XML
        public DateTime FechaVcto { get; set; }       // movementDate
        public String GLNComprador { get; set; }     // //buyer/gln  ** 
        public String UniqueId { get; set; }        // typedEntityIdentification/entityIdentification/uniqueCreatorIdentification
        public String SalesDepartament { get; set; }    // salesDepartamentNumber
        public string Proceso { get; set; } // Sólo Debug, guarda si docto fue procesado, objetado
    }
    
}







