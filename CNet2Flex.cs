using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Xml.Linq;
using System.Data.SqlClient;
using System.Globalization;
using System.Reflection;

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
                var currentDirectory = AppDomain.CurrentDomain.BaseDirectory;
                var settingsXMLFilepath = Path.Combine(currentDirectory, filenameXMLSettings);

                oLog = new App_log(currentDirectory);
                oLog.Add("DEBUG", "======== Inicio Proceso ========");
                oLog.Add("TRACE", "Get Settings: " + settingsXMLFilepath);

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
                
                if (Params.RutSociedades.Count == 0)
                {
                    oLog.Add("ERROR", "Sociedades no encontradas, revise estructura XML");
                }
                else
                {
                    oLog.Add("TRACE", "Get Settings Successed");

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
         try{
                CNET2Flex C2F = new CNET2Flex();
                
                // TODO: Leer LP, Producto, EAN Desde Flexline
                string[] filesXmlinDirCNET = Directory.GetFiles(@Params.DirectorioFTPCNET, "*.*");
                oLog.Add("TRACE", String.Format("{0} archivos encontrados ", filesXmlinDirCNET.Count()));
                
                int Correlativo = 1;
                foreach (string file in filesXmlinDirCNET)
                {
                    string onlyfileName = file.Substring(Params.DirectorioFTPCNET.Length + 1).Replace(".xml", "");
                    string[] ArrayFileName = onlyfileName.Split(".");

                    if (ReadFileCNETAsync(file, Correlativo))
                    {
                        if(ReCalcAndSave(ArrayFileName))
                        {
                                Correlativo ++;
                                // Mover archivos
                                string DirProcessed = Path.Combine(@Params.DirectorioFTPCNET , "Procesados");
                                CreateDirectory(DirProcessed);

                                oLog.Add("TRACE",
                                        String.Format("Terminado: Moviendo archivo {0} a directorio procesados",
                                        file.Substring(@Params.DirectorioFTPCNET.Length + 1)));

                                File.Move(file, Path.Combine(DirProcessed, file.Substring(@Params.DirectorioFTPCNET.Length + 1)), true);

                        } else {
                            oLog.Add("ERROR", String.Format("No logró leer XML {0} correctamente.", file));
                        }   
                    } else {
                            oLog.Add("ERROR", String.Format("No pudo leer xml {0} ", file));
                    } 

                    // if (!ReadFileCNETAsync(file, Correlativo))
                    // {
                    //     oLog.Add("ERROR", String.Format("No pudo leer xml {0} ", file));
                    // }
                    // // Recalculo de valores
                    // if(!ReCalcAndSave(ArrayFileName))
                    // {
                    //     oLog.Add("ERROR", String.Format("No logró leer XML {0} correctamente.", file));
                    // }

                    // TODO: Habilitar correo
                    //SendEmail();
                    // TODO: Mover archivo procesado con éxito

                    
                }
                    // Sólo recopilar Info en Tabla Dinámica
                    /*
                    oLog.Add("HEADER", String.Format("{0} ", ToCsvHeader(DbTable[0])));
                    foreach (var DbTable in DbTable)
                    {
                        var output1 = ToCsvRow(DbTable);
                        oLog.Add("CEC", String.Format("{0} ", output1));
                    }
                    
                    oLog.Add("DETALLE", String.Format("{0} ", ToCsvHeader(DbDetail[0])));
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
            var Dbt = DbTable.Where(x => x.GLN == ArrayFileName[0].ToString() && x.Numero == ArrayFileName[2].ToString());
            if (Dbt.Count() == 0) return false;

            foreach(var DbTable in DbTable.Where(x => x.GLN == ArrayFileName[0].ToString() && x.Numero == ArrayFileName[2].ToString()))
            {           
                // DataRowCollection RowsGets = GetCodigoProductoSispal(DbTable);
                
                // if(RowsGets.Count == 0)
                // {
                //     oLog.Add("INFO", String.Format("No encontró registros en Sispal"));
                // }

                // foreach(DataRow fila in RowsGets)
                // {
                //     foreach(var DbDetail in DbDetail.Where(x => x.Item == fila[0].ToString()))
                //     {
                //         // TODO: Producto Generico en Flexline
                //         DbDetail.ItemFlexlineLPCNet = fila[1].ToString() != "" ? fila[1].ToString(): "No Existe!";  
                //         DbDetail.ItemColorLPCNet = fila[2].ToString();
                //         DbDetail.GlosaLPCnet = fila[3].ToString() != "" ? fila[3].ToString(): "Producto No Existe en Syspal o no posee código Flexline";
                //         DbDetail.PrecioLPCNET = Convert.ToDouble(fila[4]);
                //         // Precio 4, UnidadContenedora 5, EDI 6
                //     }

                // }

                DataRowCollection RowsGets = GetCodigoProductoFlexline(DbTable);
                if(RowsGets.Count == 0)
                {
                    oLog.Add("INFO", String.Format("No encontró registros en ListaPrecios CNET / Flexline"));
                }

                foreach(DataRow fila in RowsGets)
                {
                    foreach(var DbDetail in DbDetail.Where(x => x.Item == fila["ItemUpc"].ToString()))
                    {
                        
                        DbDetail.ItemFlexlineLPCNet = fila["ItemFlexline"].ToString() != "" ? fila["ItemFlexline"].ToString(): "No Existe!";  
                        DbDetail.GlosaLPCnet = fila["GlosaLPCnet"].ToString() != "" ? fila["GlosaLPCnet"].ToString(): "Producto No Existe en LPCNet o no posee código Flexline";

                        DbDetail.ItemColorLPCNet = fila["ItemColor"].ToString();

                        DbDetail.PrecioLPCNET = Convert.ToDouble(fila["Precio"]);
                        DbDetail.ListaPrecioFlexline = fila["ListaPrecio"].ToString();
                        DbDetail.PrecioFlexline = Math.Round(Convert.ToDouble(fila["PrecioLPFlexline"]));
                        DbDetail.FechaInicio = Convert.ToDateTime(fila["Fec_Inicio"].ToString());
                        DbDetail.FechaFin = Convert.ToDateTime(fila["Fec_Final"].ToString());
                        DbDetail.UnidadFlexline = fila["Unidad"].ToString();
                        DbDetail.VigenciaProductoFlexline = fila["Vigente"].ToString();

                        DbDetail.CantidadConvertidaFlexline = Math.Round(DbDetail.Cantidad * (DbDetail.UnidadContenida == 0 ? 1:DbDetail.UnidadContenida)) ;
                        DbDetail.PrecioConvertidoFlexline = Math.Round(DbDetail.Precio / (DbDetail.UnidadContenida == 0 ? 1:DbDetail.UnidadContenida)) ; 

                        DbDetail.TotalConvertidoFlexline += Math.Round(DbDetail.CantidadConvertidaFlexline * DbDetail.PrecioConvertidoFlexline);

                    }
                    
                }
                
                // Llena Observaciones y Normaliza Diferencias
                //foreach(var DbDetail in DbDetail)
                foreach (DocumentoD DbDetail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                {
                    if (DbDetail.ItemFlexlineLPCNet == null)  // TODO: Verificar vacio, Null y Empty No encuentra Item
                    {
                        DbDetail.GlosaLPCnet = "Producto No Existe en LPCNet o no posee código Flexline";
                        DbDetail.CantidadConvertidaFlexline = DbDetail.Cantidad;
                        DbDetail.PrecioConvertidoFlexline = DbDetail.Precio;
                        DbDetail.TotalConvertidoFlexline += DbDetail.CantidadConvertidaFlexline * DbDetail.PrecioConvertidoFlexline;
                    }

                    DbDetail.Observaciones = ""; 
                    DbDetail.Observaciones += DbDetail.ItemFlexlineLPCNet == null ? "- Producto no Existe\n":"";
                    DbDetail.Observaciones += DbDetail.UnidadContenida == 0? "- Unidad de Empaque no encontrada\n":"";
                    DbDetail.Observaciones += (DbTable.Fecha <= DbDetail.FechaInicio || DbTable.Fecha >= DbDetail.FechaFin)? "- Lista de Precios vencida\n":"";
                    DbDetail.Observaciones += DbDetail.VigenciaProductoFlexline != "S"? "- Producto no Vigente en Flexline \n":"";
                    DbDetail.Observaciones += DbDetail.PrecioConvertidoFlexline != DbDetail.PrecioFlexline? "- Precio distinto a Lista de Precios Flexline \n":"";

                    // Normaliza ItemFlexline sólo si no encuentra
                    DbDetail.ItemFlexlineLPCNet = DbDetail.ItemFlexlineLPCNet == null ? "No Existe!": DbDetail.ItemFlexlineLPCNet; 

                    // Calculo de Totaless
                    DbTable.Total +=  Math.Round(DbDetail.TotalConvertidoFlexline);

                    // //Asigna LP (Asume que todas las líneas del docto traen el mismo dato a partir de rev. de archivos 2020)
                    // DbTable.ListaPrecioCNET = DbDetail.ItemColor == null ? "": DbDetail.ItemColor;

                    // Sólo Tabla Dinámica, Quitar con refactor
                    DbDetail.Fecha = DbTable.Fecha;
                    DbDetail.Ctacte = DbTable.Ctacte;
                    DbDetail.NombreCliente = DbTable.NombreCliente;
                    DbDetail.CondPago = DbTable.CondPago;
                    DbDetail.GLN = DbTable.GLN;
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

                }
                
                int Resp = Program_Write(DbTable);
            }
            return true;
        }
        static bool ReadFileCNETAsync(string filename, int Correlativo)
        {
            oLog.Add("TRACE", String.Format("Leyendo Archivo {0} ", filename));
            try
            {
                //  El  archivo se ciñe al siguiente formato:
                // <Casilla EDI Emisor>.<Casilla EDI Receptor>.<Nodocumento>.<función>.<año><mes><día><hora><minuto>
                string onlyfileName = filename.Substring(Params.DirectorioFTPCNET.Length + 1).Replace(".xml", "");
                string[] ArrayFileName = onlyfileName.Split(".");

                var CtacteFlex= Params.CtacteComercioNet2Flex.Find(p => p[0] == ArrayFileName[0]);
                string Ctacte = CtacteFlex == null? "": CtacteFlex[1];
                string NombreCliente = CtacteFlex == null? "": CtacteFlex[2];
                string CondPagoCliente = CtacteFlex == null? "": CtacteFlex[3];

                var EmpresaFlex= Params.RutSociedades.Find(p => p[3] == ArrayFileName[1]);
                string Empresa = EmpresaFlex == null? "": EmpresaFlex[2];

                XElement Xml4LINQ = XElement.Load(filename);  

                // Caso especial
                // =============
                // Hay Archivos que traen NO XNamespace homogeneo en todas las ramas a diferencia de Wallmart
                // vienen sólo en algunas ramas: "transaction" "documentCommand" "order"
                // Se implementa regla para su lectura sólosobre esas ramas
                // Obs: Si hay otros casos se debe implementar la regla en código!!!!
                
                XNamespace aw = "http://www.uc-council.org/smp/schemas/eanucc"; 
                string EDIWalmart = "925485K200";
                XNamespace awNone = ArrayFileName[0] == EDIWalmart ?"http://www.uc-council.org/smp/schemas/eanucc":XNamespace.None; 

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
                        GLN = ArrayFileName[0],
                        GLNStarfood = ArrayFileName[1],
                        Ctacte = Ctacte,
                        NombreCliente = NombreCliente,
                        Empresa = Empresa,
                        Correlativo = Correlativo,
                        CondPago = CondPagoCliente,
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
                        var DirDespacho= Params.GLNlocacion.Find(p => p.GLNDespacho == Encabezado.GLNDireccionDespacho);
                        Encabezado.DireccionDespacho = DirDespacho == null? "": DirDespacho.Direccion + " " + DirDespacho.Comuna;

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
                    select new DocumentoD {
                        Empresa = Empresa,
                        Numero = ArrayFileName[2],
                        Correlativo = Correlativo,

                        Linea = (int)el.Attribute("number"),
                        Precio = (Double)el.Element(awNone + "netPrice").Element(awNone + "amount"),
                        ListaPrecio = (string)el.Element(awNone + "PriceType"),

                        Cantidad = (Double)el.Element(awNone + "requestedQuantity"),
                        CantidadUnitType = (string)el.Element(awNone + "requestedQuantity").Attribute("UnitType"),

                        UnidadContenida = (Double)el.Element(awNone + "containedUnits"),
                        UnidadContenidaUnitType = (string)el.Element(awNone + "containedUnits").Attribute("UnitType"),

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
                    String.Format("Error al obtener DTE's en {0}", ex.Message));
                return false;
            }

        }
        public static void SendEmail() //IEnumerable<Documento> Docto, IEnumerable<DocumentoD> DoctoD)
        {

            MailMessage Mensaje = new MailMessage();
            Mensaje.To.Add(new MailAddress(Params.EmailTO));
            Mensaje.To.Add(new MailAddress(Params.EmailTO2));
            Mensaje.From = new MailAddress(Params.EmailUser);
        
            Mensaje.IsBodyHtml = true;

            //TODO: Indicar nombre de Proveedor desde CtacteComercioNet2Flex
            Mensaje.Subject = String.Format("Orden de Compra ComercioNet - {0}", DbTable[0].NombreCliente);

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
            Texto += "<p>A Continuaci&oacute;n se presenta el detalle de la Orden de Compra de la empresa {0} por un total del ${1} registrada en ComercioNet el d&iacute;a de hoy.&nbsp;</p>";

            string Normal = "<p>El Documento no presenta diferencias y est&aacute; listo para su proceso de Integraci&oacute;n con ERP Flexline, favor proceder.</p>";
            string Warning = "<p><span style='color: #ff0000;'>*** El Documento está listo para su proceso de integraci&oacute;n, pero posee diferencias (quedar&aacute; con estado pendiente).&nbsp; Favor revisar y proceder. ***</span></p>";

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
            EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Depto. Ventas:</span></td>";
            EncabezadoPrincipal += "<td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{8}</span></td>";
            EncabezadoPrincipal += "<td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Tipo OC:</span></td>";
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
        
            // string EncabezadoSecundario = "";
            // EncabezadoSecundario += "<table style='border-collapse:collapse;border-spacing:0' class='tg'>";
            // EncabezadoSecundario += "    <thead>";
            // EncabezadoSecundario += "        <tr>";
            // EncabezadoSecundario += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Departamento de Ventas:</span></th>";
            // EncabezadoSecundario += "            <th style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'>{0}</th>";
            // EncabezadoSecundario += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:bold;text-decoration:none'>Tipo de Orden de Compra:</span></th>";
            // EncabezadoSecundario += "            <th style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'>{1}</th>";
            // EncabezadoSecundario += "        </tr>";
            // EncabezadoSecundario += "    </thead>";
            // EncabezadoSecundario += "    <tbody>";
            // EncabezadoSecundario += "        <tr>";
            // EncabezadoSecundario += "            <td style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Promoción:</span></td>";
            // EncabezadoSecundario += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'>{2}</td>";
            // EncabezadoSecundario += "            <td style='background-color:#96fffb;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Número de Proveedor</span></td>";
            // EncabezadoSecundario += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:10px 5px;text-align:left;vertical-align:top;word-break:normal'><span style='text-decoration:none'>{3}</span></td>";
            // EncabezadoSecundario += "        </tr>";
            // EncabezadoSecundario += "    </tbody>";
            // EncabezadoSecundario += "</table>";

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
            DetalleHead += "            <th style='background-color:#EEEEEE;border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'><span style='font-weight:700;font-style:normal'>Precio Unit.</span></th>";
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
            DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'>{9}</td>";
            DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:center;vertical-align:top;word-break:normal'>{10}</td>";
            DetalleItem += "            <td style='border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:11px;overflow:hidden;padding:6px 5px;text-align:left;vertical-align:top;word-break:normal;color:red'>{11}</td>";
            DetalleItem += "        </tr>";

            string footerTabla = @"
                    <td style='color: #ffffff; font-family: Arial, sans-serif; font-size: 10px;'>
                    &reg; Powered by Codevsys 2020 para Starfood <br/>
                    </td>";

            string Cliente;
            string EmpresaStarfood;
            string DireccionDespacho;

            var Nombre = Params.GLNlocacion.Find(z => z.GLNDespacho == DbTable[0].DireccionDespacho);
            Cliente = Nombre == null? "Starfood": Nombre.Nombre;
            
            var NomStarfood = Params.RutSociedades.Find(z => z[3] == DbTable[0].GLNStarfood);
            EmpresaStarfood = NomStarfood == null? "Starfood": NomStarfood[1];

            var DirDespacho = Params.GLNlocacion.Find(z => z.GLNDespacho == DbTable[0].DireccionDespacho);
            DireccionDespacho = DirDespacho == null? "No Existe en Definición de Starfood": DirDespacho.Direccion + " " + DirDespacho.Comuna;

            EncabezadoPrincipal = String.Format(EncabezadoPrincipal, 
                                Cliente, EmpresaStarfood, DbTable[0].Numero, DbTable[0].Fecha, 
                                DbTable[0].Fecha, DbTable[0].FechaVcto, DbTable[0].PlazoPago + " " + DbTable[0].TipoPlazoPago,
                                DireccionDespacho,
                                DbTable[0].SalesDepartament, DbTable[0].TipoOC, 
                                DbTable[0].Promocion, DbTable[0].NroInternoProveedor);

            // EncabezadoSecundario = String.Format(EncabezadoSecundario, 
            //        DbTable[0].SalesDepartament, DbTable[0].TipoOC, 
            //         DbTable[0].Promocion, DbTable[0].NroInternoProveedor);
                    

            string DetalleItemDocumento = "";
            string Obs = "";
            Double TotalDocto = 0;
            
            foreach (DocumentoD Detail in DbDetail)
            {
                //Conversiones
                // Obs = Detail.UnidadContenida == 0? "Unidad de Empaque no encontrada":"";
                // CantidadFlex = Detail.Cantidad * (Detail.UnidadContenida == 0 ? 1:Detail.UnidadContenida) ;
                // PrecioFlex = Detail.Precio / (Detail.UnidadContenida == 0 ? 1:Detail.UnidadContenida) ;
                
                TotalDocto += Detail.Total;
                //TotalDoctoConvertido += Detail.TotalConvertidoFlexline;
                Obs += Detail.Observaciones;

                DetalleItemDocumento += String.Format(DetalleItem,
                    Detail.Linea, Detail.Item, Detail.ItemFlexlineLPCNet, Detail.GlosaLPCnet, Detail.ItemSize,
                    Detail.ItemColor, Detail.CantidadConvertidaFlexline, Detail.PrecioConvertidoFlexline.ToString("#,##0"), Detail.UnidadContenida,
                    Detail.Cantidad.ToString("#,##0.00") , Detail.TotalConvertidoFlexline.ToString("#,##0"), Detail.Observaciones.Replace("\n","<br>")
                    );
                
            }

            DetalleItemDocumento += "        </tbody>";
            DetalleItemDocumento += "</table>";

            Texto = String.Format(Texto, 
                    Cliente, Math.Round(TotalDocto).ToString("#,##0"));

            Mensaje.Body = Texto + (Obs == ""? Normal : Warning) 
                         +"<p>&nbsp;</p>" + EncabezadoPrincipal + "<p>&nbsp;</p>" 
                         + DetalleHead + DetalleItemDocumento 
                         + footerTabla;
                
            smtp.Send(Mensaje);

            oLog.Add("INFO", String.Format("Email enviado con {0} registros informados", "0"));



        }
        static public string[] GetDirectory()
        {
            ServicePointManager.ServerCertificateValidationCallback += (o, c, ch, er) => true;

            //StringBuilder result = new StringBuilder();
            //FtpWebRequest requestDir = (FtpWebRequest)WebRequest.Create(@Params.FTPServer);
            //requestDir.Method = WebRequestMethods.Ftp.ListDirectory;
            //requestDir.Credentials = new NetworkCredential(Params.FTPUser, Params.FTPPassword);
            //requestDir.EnableSsl = true;
            //FtpWebResponse responseDir = (FtpWebResponse)requestDir.GetResponse();
            //StreamReader readerDir = new StreamReader(responseDir.GetResponseStream());



            FtpWebRequest directoryListRequest = (FtpWebRequest)WebRequest.Create(Params.FTPServer);

            directoryListRequest.Credentials = new NetworkCredential(Params.FTPUser, Params.FTPPassword);
            directoryListRequest.EnableSsl = true;
            directoryListRequest.UsePassive = true;
            directoryListRequest.KeepAlive = false;

            directoryListRequest.Method = WebRequestMethods.Ftp.ListDirectory;
            
            //FtpWebResponse response = (FtpWebResponse)directoryListRequest.GetResponse();
            //StreamReader streamReader = new StreamReader(response.GetResponseStream());

            //List<string> directories = new List<string>();

            //string line = streamReader.ReadLine();
            ////Obtiene el contenido y lo agrega al List<string>.
            //while (!string.IsNullOrEmpty(line))
            //{
            //    directories.Add(line);
            //    line = streamReader.ReadLine();
            //}

            //directoryListRequest.Close();


            using (FtpWebResponse directoryListResponse = (FtpWebResponse)directoryListRequest.GetResponse())
            {
                using (StreamReader directoryListResponseReader = new StreamReader(directoryListResponse.GetResponseStream()))
                {
                    string responseString = directoryListResponseReader.ReadToEnd();
                    string[] results = responseString.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                    return results;
                }
            }
        }
        static public int Program_Write(Documento DbTable)
            {
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
                            + " @empresa, @tipodocto, @correlativo, '' ctacte, 0 numero, @fecha, '' proveedor, @cliente, @bodega, '' bodega2, "
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
                            command.Parameters.AddWithValue("@tipodocto", "NOTA DE VENTA");
                            command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);
                            command.Parameters.AddWithValue("@fecha", DbTable.Fecha);
                            command.Parameters.AddWithValue("@cliente", DbTable.Ctacte);
                            command.Parameters.AddWithValue("@bodega", "01 LAG SUR");
                            command.Parameters.AddWithValue("@fechavcto", DbTable.Fecha.AddDays(DbTable.PlazoPago));   // TODO: Calcular Fechavcto
                            command.Parameters.AddWithValue("@listaprecio",  DbTable.CondPago);   // TODO: ListaPrecios

                            command.Parameters.AddWithValue("@neto", Math.Round(DbTable.Total));   // TODO: ListaPrecios
                            command.Parameters.AddWithValue("@subtotal", Math.Round(DbTable.Total));   // TODO: 
                            command.Parameters.AddWithValue("@total", Math.Round(DbTable.Total * 1.19));   // TODO: Ir a buscar Parametro de Iva
                            command.Parameters.AddWithValue("@netoingreso", Math.Round(DbTable.Total));   // TODO: 
                            command.Parameters.AddWithValue("@subtotalingreso", Math.Round(DbTable.Total));   // TODO: 
                            command.Parameters.AddWithValue("@totalingreso", Math.Round(DbTable.Total * 1.19));   // TODO: 
                            command.Parameters.AddWithValue("@periodolibro", DbTable.Fecha.Year*100 + DbTable.Fecha.Month);   // TODO: 

                            command.Parameters.AddWithValue("@aprobacion", "S");   // TODO: 
                            command.Parameters.AddWithValue("@idctacte", DbTable.Ctacte);
                            command.Parameters.AddWithValue("@glosa", "S");   // TODO: 
                            command.Parameters.AddWithValue("@comentario1", DbTable.UniqueId);   // TODO: OC Nro.
                            command.Parameters.AddWithValue("@comentario2", DbTable.ArchivoCNet);   // TODO: OC Nro.
                            command.Parameters.AddWithValue("@nromensaje", 29);   // TODO: ???? no me acuerdo que es

                            command.Parameters.AddWithValue("@direccion", DbTable.DireccionDespacho);   // TODO: 
                            command.Parameters.AddWithValue("@ciudad", "");   // TODO: 
                            command.Parameters.AddWithValue("@comuna", "");   // TODO: 

                            command.Parameters.AddWithValue("@hora", DateTime.Now.GetDateTimeFormats('t')[0]);   // TODO: 

                            command.Parameters.AddWithValue("@netobimoneda", Math.Round(DbTable.Total));   // TODO: 
                            command.Parameters.AddWithValue("@subtotalbimoneda", Math.Round(DbTable.Total));   // TODO: 
                            command.Parameters.AddWithValue("@totalbimoneda", Math.Round(DbTable.Total * 1.19));   // TODO: 

                            command.Parameters.AddWithValue("@analisise5", DbTable.Numero);   // TODO: IDUnique
                            command.Parameters.AddWithValue("@analisise6", DbTable.Fecha.ToShortDateString());   // TODO: 

                            int Rows = command.ExecuteNonQuery();  // TODO: Enviar Log con cnt Registros
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
                                + " @empresa, @tipodocto, @correlativo, @secuencia, @linea, @producto, @cantidad, @precio, 0 porcentajedr, @subtotal, "
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
                                + " '' analisis17, '' analisis18, '' analisis19, '' analisis20, @unimeddynamic, '' prodalias, '' fechavigencialp, '' lotedestino, '' seriedestino, 'N' doctoorigenval, "
                                + " 0 drglobal1, 0 drglobal2, 0 drglobal3, 0 drglobal4, 0 drglobal5, 0 drglobal1ingreso, 0 drglobal2ingreso, 0 drglobal3ingreso, 0 drglobal4ingreso, 0 drglobal5ingreso, "
                                + " 0 drglobal1bimoneda, 0 drglobal2bimoneda, 0 drglobal3bimoneda, 0 drglobal4bimoneda, 0 drglobal5bimoneda, 0 porcentajedr2, 0 porcentajedr3, 0 porcentajedr4, 0 porcentajedr5, 0 valporcentajedr1, "
                                + " 0 valporcentajedr2, 0 valporcentajedr3, 0 valporcentajedr4, 0 valporcentajedr5, 0 valporcentajedr1ingreso, 0 valporcentajedr2ingreso, 0 valporcentajedr3ingreso, 0 valporcentajedr4ingreso, 0 valporcentajedr5ingreso, 0 valporcentajedr1bimoneda, "
                                + " 0 valporcentajedr2bimoneda, 0 valporcentajedr3bimoneda, 0 valporcentajedr4bimoneda, 0 valporcentajedr5bimoneda, 0 costobimoneda, 0 cupbimoneda, 0 montoasignado, '' analisis21, '' analisis22, '' analisis23, "
                                + " '' analisis24, '' analisis25, '' analisis26, '' analisis27, '' analisis28, '' analisis29, '' analisis30, '' receta, 0 cuotacontrato, 0 secuenciakit, "
                                + " 0 iddocto, 0 montoasignadoingreso, 'Producto' origeningreso, @origencodigo, 0 totalrecar, 0 descprorra, 0 idseg_empresa, 0 idtipodocumento, 0 idproducto, 0 iddoctodet ";

                            command.CommandText = Sqltext + " " + SqltextDet;    
                            int Linea = 1;
                            foreach (DocumentoD DbDetail in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                            {
                                
                                command.Parameters.Clear();
                                command.Parameters.AddWithValue("@empresa",  DbTable.Empresa);
                                command.Parameters.AddWithValue("@tipodocto", "NOTA DE VENTA");
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
                                command.Parameters.AddWithValue("@unidadingreso", DbDetail.UnidadFlexline == null ?"UN":DbDetail.UnidadFlexline);
                                command.Parameters.AddWithValue("@cantidadingreso", DbDetail.CantidadConvertidaFlexline);
                                command.Parameters.AddWithValue("@precioingreso", DbDetail.PrecioConvertidoFlexline);
                                command.Parameters.AddWithValue("@subtotalingreso", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@netoingreso", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@totalingreso", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@bodega", "01 LAG SUR");
                                command.Parameters.AddWithValue("@fechaentrega", DbTable.Fecha.AddDays(DbTable.PlazoPago));
                                command.Parameters.AddWithValue("@fecha", DbTable.Fecha);

                                command.Parameters.AddWithValue("@comentario", DbDetail.Observaciones);
                                command.Parameters.AddWithValue("@vigente", "S");  // TODO: Ver

                                command.Parameters.AddWithValue("@preciobimoneda", DbDetail.PrecioConvertidoFlexline);
                                command.Parameters.AddWithValue("@subtotalbimoneda", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@netobimoneda", DbDetail.TotalConvertidoFlexline);
                                command.Parameters.AddWithValue("@totalbimoneda", DbDetail.TotalConvertidoFlexline);

                                command.Parameters.AddWithValue("@preciolistap", DbDetail.PrecioFlexline);

                                command.Parameters.AddWithValue("@unimeddynamic", 0);
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
                            command.Parameters.AddWithValue("@tipodocto", "NOTA DE VENTA");
                            command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);

                            command.Parameters.AddWithValue("@codigopago", DbTable.CondPago);
                            command.Parameters.AddWithValue("@fechavcto", DbTable.Fecha.AddDays(DbTable.PlazoPago));
                            command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * 1.19));   // TODO: 
                            command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * 1.19));   // TODO: 
                            
                            command.Parameters.AddWithValue("@tipodoctopago", "NOTA DE VENTA");
                            command.Parameters.AddWithValue("@nrodoctopago", DbTable.Numero);
                            command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * 1.19));   // TODO: 
                            command.Parameters.AddWithValue("@fechavctodocto", DbTable.Fecha.AddDays(DbTable.PlazoPago));

                            command.Parameters.AddWithValue("@montopago", Math.Round(DbTable.Total * 1.19));   // TODO: 

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
                                command.Parameters.AddWithValue("@tipodocto", "NOTA DE VENTA");
                                command.Parameters.AddWithValue("@correlativo", DbTable.Correlativo);
                                command.Parameters.AddWithValue("@orden", i);

                                switch (i)
                                {
                                    case 1:
                                        command.Parameters.AddWithValue("@nombre", "Neto");
                                        command.Parameters.AddWithValue("@factor", 0);
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * 1.19));
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * 1.19));
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * 1.19));
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
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * 1.19));
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * 1.19));
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * 1.19));
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
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * 1.19));
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * 1.19));
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * 1.19));
                                        break;
                                    default:
                                        command.Parameters.AddWithValue("@nombre", "IVA");
                                        command.Parameters.AddWithValue("@factor", 1);
                                        command.Parameters.AddWithValue("@monto", Math.Round(DbTable.Total * 0.19));
                                        command.Parameters.AddWithValue("@montoingreso", Math.Round(DbTable.Total * 0.19));
                                        command.Parameters.AddWithValue("@montobimoneda", Math.Round(DbTable.Total * 0.19));
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
        {
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionSisPal;

                        connection.Open();
                        string SqlText = "";
                        
                        // "Select Isnull(Embalaje_Cod_Barras,'') Embalaje_Cod_Barras, Isnull(Producto,'') Producto, "
                        // + "Isnull(Producto_Cod_Barras,'') Producto_Cod_Barras, Isnull(Glosa,'') Glosa " 
                        // + "FROM SP_PRODUCTO a " 
                        // + "Where " 
                        // + "a.Empresa = @Empresa and a.Embalaje_Cod_Barras in ({0}) ";

                        SqlText = "Select Isnull(ItemUpc,'') ItemUpc, Isnull(ItemFlexline,'') Producto, "
                            + " ItemColor, Isnull(Descripcion,'') Glosa, Precio, UnidadContenedora, EDI "
                            + " FROM ListaPrecioCnet a "
                            + " Where "
                            + " a.Empresa = @Empresa and a.ItemUpc in ({0}) ";

                        string Items = "";
                        //foreach (DocumentoD Row in DbDetail)
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
         public static DataRowCollection GetCodigoProductoFlexline(Documento DbTable)
        {
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionFlexline;

                        connection.Open();
                         string SqlText = "";
                        //  SELECT a.Producto, Isnull(cta.Listaprecio,0) Listaprecio, Isnull(Lpd.Valor,0) Valor, " 
                        // + "Isnull(Lp.Fec_Inicio,'') Fec_Inicio, Isnull(Fec_Final,'') Fec_Final, Isnull(a.Unidad,'') Unidad, " 
                        // + "Isnull(a.Vigente,'N') Vigente " 
                        // + "FROM Producto a " 
                        // + "Left join Ctacte cta on cta.empresa=a.empresa and cta.Ctacte = @Ctacte and cta.Tipoctacte='Cliente' "
                        // + "Left Join ListaPrecio Lp on Lp.Empresa=a.Empresa "
                        // + "and ( (Lp.LisPrecio = @PrefijoLp and @PrefijoLp <> '') or (Lp.LisPrecio =Cta.Listaprecio and @PrefijoLp = '') "  //TODO: Ver lógica final
                        // //+ "and ( (Lp.LisPrecio = @PrefijoLp and @PrefijoLp <> '') or (Lp.LisPrecio =Cta.Listaprecio and @PrefijoLp = '') "  //TODO: Ver lógica final
                        // + "Left Join ListaPreciod Lpd on Lpd.Empresa=a.Empresa and Lpd.IdLisPrecio=Lp.IdLisPrecio and Lpd.Producto=a.Producto "
                        // + "Where " 
                        // + "a.Empresa = @Empresa and a.Producto in ({0}) ";


                         SqlText = "Select a.ItemUpc, a.ItemFlexline, a.Descripcion GlosaLPCnet, a.Precio, a.UnidadContenedora, a.ListaPrecio, a.ItemColor, "
                        + " Isnull(Lpd.Valor,0) PrecioLPFlexline, p.Glosa GlosaFlexline, "
                        + " Isnull(Lp.Fec_Inicio,'') Fec_Inicio, Isnull(Fec_Final,'') Fec_Final, Isnull(p.Unidad,'') Unidad, " 
                        + " Isnull(p.Vigente,'N') Vigente " 
                        + " From ListaprecioCnet a "
                        + " Left join Ctacte cta on cta.empresa=a.empresa and cta.Ctacte = @Ctacte and cta.Tipoctacte='Cliente' "
                        + " Left Join Producto p on p.Empresa= a.Empresa and p.Producto=a.ItemFlexline "
                        + " Left Join ListaPrecio lp on Lp.Empresa=a.Empresa and lp.LisPrecio=a.ListaPrecio "
                        + " Left Join ListaPrecioD Lpd on Lpd.Empresa=lp.Empresa and lpd.IdLisPrecio=Lp.IdLisPrecio and lpd.Producto=a.ItemFlexline "
                            
                        + " Where a.Empresa= @Empresa and a.ItemColor = @ItemColor and a.ItemUpc in ({0}) ";

                        string Items = "";
                        string PreFijoLP = "";
                        foreach (DocumentoD Row in DbDetail.Where(x => x.Empresa == DbTable.Empresa && x.Numero == DbTable.Numero))
                        {
                            //Items += "'" + Row.ItemFlexlineLPCNet + "',";
                            Items += "'" + Row.Item + "',";
                            PreFijoLP = Row.ItemColor;
                        }
                        
                        Items = Items != ""? Left(Items, Items.Length - 1): "''";

                        SqlText = String.Format(SqlText, Items);
                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Ctacte", DbTable.Ctacte);
                        ComandoSQL.Parameters.AddWithValue("Empresa", DbTable.Empresa);

                        // string PreFijoLP = "";
                        // if(DbTable.ListaPrecioCNET != "")
                        // {
                        //     PreFijoLP = DbTable.ListaPrecioCNET == "BOL"?"LIDER 4": "WALMART MAYOR-2";  // TODO: Cambiar por LP Correcta
                        // } 
                        ComandoSQL.Parameters.AddWithValue("ItemColor", PreFijoLP);

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
        // Tools
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
    public class Documento
    {
        public String Empresa { get; set; }     // Posición 2 RutSociedades
        public int Correlativo { get; set; }                  // Correlativo para inyección Flex
        public String Ctacte { get; set; }     // Posición 1 CtacteComercioNet2Flex
        public String NombreCliente { get; set; }     // Posición 2 CtacteComercioNet2Flex
        public String CondPago { get; set; }     // Desde Tabla Ctacte Flexline
        public DateTime Fecha { get; set; }           // creationDate
        public String Numero { get; set; }          // Desde Nombre de archivo
        public String GLN { get; set; }             // Desde Nombre de archivo Casilla EDI
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
        public Double Total { get; set; }   //Calculado 
        public string ArchivoCNet { get; set; }    // Nombre Archivo
        public string ListaPrecioCNET { get; set; }    // Lista de Precios desde XML, extrae ItemColor desde 1ra. Linea de detalle (se asume que es única a partir de revisión de archivos 2020)
    }
    public class DocumentoD
    {
        public String Empresa { get; set; }                  // DbTable.Empresa
        public int Correlativo { get; set; }                  // Correlativo para inyección Flex
        public String Numero { get; set; }                  // DbTable.Numero
        public int Linea { get; set; }                  // lineItem/@number
        public Double Precio { get; set; }                 // PrecioXML / UnidadContenida
        public String ListaPrecio { get; set; }         // PriceType
        public Double Cantidad { get; set; }               // CantidadXML * UnidadContenida
        public String CantidadUnitType { get; set; }    // requestedQuantity/@UnitType
        public Double UnidadContenida { get; set; }    // containedUnits
        public String UnidadContenidaUnitType { get; set; }    // containedUnits/@UnitType
        public String Item { get; set; }                // itemIdentification/gtin
        public String ItemBuyer { get; set; }           // itemIdentification/buyerItemNumber
        public String ItemVendor { get; set; }          // itemIdentification/vendorItemNumber
        public String ItemSize { get; set; }            // itemSize
        public String ItemColor { get; set; }            // itemColor
        public String Descripcion { get; set; }         // itemDescription/text
        public Double Total { get; set; }                  // totalAmount/amount
        public String ItemFlexlineLPCNet { get; set; }        // LPCnet ItemFlexline
        public String ItemColorLPCNet { get; set; }          // LPCnet Item_Color
        public String GlosaLPCnet { get; set; }            // LpCnet Descripcion
        public Double PrecioLPCNET { get; set; }         // LpCnet Precio
        public String ListaPrecioFlexline { get; set; }    // Flexline/Ctacte/Listaprecio
        public Double PrecioFlexline { get; set; }         // Flexline/ListaPrecio/Valor
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
        public String GLN { get; set; }             // Desde Nombre de archivo Casilla EDI
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

    }

}
