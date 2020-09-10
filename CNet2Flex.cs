using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Net.Mail;
using System.Xml;
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

            CNET2Flex C2F = new CNET2Flex();
            //DataTable precios = OpenSqlConnection(SqlText);
            //try
            //{

                // Código!!!
                // FTP de Prueba...
            //string[] direct = GetDirectory();

            // TODO: Leer XML, pasar por Clase NPedido
            // TODO: Leer LP, Producto, EAN Desde Flexline
            // TODO: Leer conversión en SisPal

            string[] filesXmlinDirCNET = Directory.GetFiles(@Params.DirectorioFTPCNET, "*.*");

            oLog.Add("TRACE", String.Format("{0} archivos encontrados... ", filesXmlinDirCNET.Count()));

            foreach (string file in filesXmlinDirCNET)
            {

                if (!ReadFileCNETAsync(file))
                {
                    oLog.Add("ERROR", String.Format("No pudo leer xml {0} ", file));
                }
                else {
                    if (DbTable.Count() == 0)
                    {
                        //     oLog.Add("DEBUG", String.Format("GLN Cliente {0} ", DbTable[0].GLN));
                        //     oLog.Add("CEC", String.Format("GLN Despacho {0} ", DbTable[0].DireccionDespacho));
                        //     foreach(var det in DbDetail)
                        //     {
                        //         oLog.Add("CEC", String.Format("EAN  {0} {1} CodFlex: {2}", det.Item, det.Descripcion, det.ItemFlexline));
                        //     }
                        // }
                        // else {
                        oLog.Add("ERROR", String.Format("No logró leer XML {0}", file));
                    }
                }
                               
                DataRowCollection Rows = GetCodigoProductoSispal();

                foreach(DataRow fila in Rows)
                {
                    foreach(var DbDetail in DbDetail.Where(x => x.Item == fila[0].ToString()))
                    {
                        // TODO: Producto Generico en Flexline
                        DbDetail.ItemFlexline = fila[1].ToString() != "" ? fila[1].ToString(): "No Existe!";  
                        DbDetail.Item_Cod_Barras = fila[2].ToString();
                        DbDetail.GlosaSisPal = fila[3].ToString() != "" ? fila[3].ToString(): "Producto No Existe en Syspal o no posee código Flexline";
                    }

                }

                Rows = GetCodigoProductoFlexline();

                foreach(DataRow fila in Rows)
                {
                    //var Detail = DbDetail.Find(x => x.ItemFlexline == fila[0].ToString());
                    foreach(var DbDetail in DbDetail.Where(x => x.ItemFlexline == fila[0].ToString()))
                    {
                        DbDetail.ListaPrecioFlexline = fila[1].ToString();
                        DbDetail.PrecioFlexline = Convert.ToDouble(fila[2]);
                        DbDetail.FechaInicio = Convert.ToDateTime(fila[3].ToString());
                        DbDetail.FechaFin = Convert.ToDateTime(fila[4].ToString());
                        DbDetail.UnidadFlexline = fila[5].ToString();
                        DbDetail.VigenciaProductoFlexline = fila[6].ToString();

                        DbDetail.CantidadConvertidaFlexline = DbDetail.Cantidad * (DbDetail.UnidadContenida == 0 ? 1:DbDetail.UnidadContenida) ;
                        DbDetail.PrecioConvertidoFlexline = DbDetail.Precio / (DbDetail.UnidadContenida == 0 ? 1:DbDetail.UnidadContenida) ; 

                        DbDetail.TotalConvertidoFlexline += DbDetail.CantidadConvertidaFlexline * DbDetail.PrecioConvertidoFlexline;
                        

                    }
                    
                }
                
                // Llena Observaciones y Normaliza Diferencias
                foreach(var DbDetail in DbDetail)
                {
                    if (DbDetail.ItemFlexline == null)  // TODO: Verificar vacio, Null y Empty No encuentra Item
                    {
                        DbDetail.GlosaSisPal = "Producto No Existe en Syspal o no posee código Flexline";
                        DbDetail.CantidadConvertidaFlexline = DbDetail.Cantidad;
                        DbDetail.PrecioConvertidoFlexline = DbDetail.Precio;
                        DbDetail.TotalConvertidoFlexline += DbDetail.CantidadConvertidaFlexline * DbDetail.PrecioConvertidoFlexline;
                    }

                    DbDetail.Observaciones = ""; 
                    DbDetail.Observaciones += DbDetail.ItemFlexline == null ? "- Producto no Existe\n":"";
                    DbDetail.Observaciones += DbDetail.UnidadContenida == 0? "- Unidad de Empaque no encontrada\n":"";
                    DbDetail.Observaciones += (DbTable[0].Fecha <= DbDetail.FechaInicio || DbTable[0].Fecha >= DbDetail.FechaFin)? "- Lista de Precios vencida\n":"";
                    DbDetail.Observaciones += DbDetail.VigenciaProductoFlexline != "S"? "- Producto no Vigente en Flexline \n":"";
                    DbDetail.Observaciones += DbDetail.PrecioConvertidoFlexline != DbDetail.PrecioFlexline? "- Precio distinto a Lista de Precios Flexline \n":"";

                    // Normaliza ItemFlexline sólo si no encuentra
                    DbDetail.ItemFlexline = DbDetail.ItemFlexline == null ? "No Existe!": DbDetail.ItemFlexline; 
                }


                // TODO: Insertar registros a Tablas Gen Flexline

                //SendEmail();
                //break;
            }

            // Sólo recopilar Info


            oLog.Add("HEADER", String.Format("{0} ", ToCsvHeader(DbTable[0])));

            foreach (var DbTable in DbTable)
            {
                //oLog.Add("CEC", String.Format("{0};{1};{2};{3};{4} ", DbTable.Empresa, DbTable.Numero, DbTable.GLN, DbTable.DireccionDespacho, DbTable.Fecha));
                var output1 = ToCsvRow(DbTable);
                //output1 += Environment.NewLine;
                oLog.Add("CEC", String.Format("{0} ", output1));
            }

            oLog.Add("DETALLE", String.Format("{0} ", ToCsvHeader(DbDetail[0])));

            foreach (var det in DbDetail)
            {
                det.Observaciones = ""; 
                var output1 = ToCsvRow(det);
                //output1 += Environment.NewLine;

                oLog.Add("CECDETALLE", String.Format("{0} ", output1));
                //oLog.Add("CEC", String.Format("{0};{1};{2};{3};{4}", det.Empresa, det.Numero, det.Item, det.Descripcion, det.ItemFlexline));
            }
                //}
                //catch (Exception ex)
                //{
                //    oLog.Add("ERROR", ex.Message);
                //}
                //finally
                //{
                //    Thread.Sleep(3000);
                //}

            }

        static bool ReadFileCNETAsync(string filename)
        {
            oLog.Add("TRACE", String.Format("Leyendo xml {0}", filename));

            try
            {
                //  El  archivo se ciñe al siguiente formato:
                // <Casilla EDI Emisor>.<Casilla EDI Receptor>.<Nodocumento>.<función>.<año><mes><día><hora><minuto>
                string onlyfileName = filename.Substring(Params.DirectorioFTPCNET.Length + 1).Replace(".xml", "");
                string[] ArrayFileName = onlyfileName.Split(".");

                var CtacteFlex= Params.CtacteComercioNet2Flex.Find(p => p[0] == ArrayFileName[0]);
                string Ctacte = CtacteFlex == null? "": CtacteFlex[1];
                string NombreCliente = CtacteFlex == null? "": CtacteFlex[2];

                var EmpresaFlex= Params.RutSociedades.Find(p => p[3] == ArrayFileName[1]);
                string Empresa = EmpresaFlex == null? "": EmpresaFlex[2];

                // TODO: Cerrar Variables!
                XElement Xml4LINQ = XElement.Load(filename);  
                XNamespace aw = "http://www.uc-council.org/smp/schemas/eanucc"; 

                // Caso Importadora
                IEnumerable<Documento> DoctoImportadora =
                    from el in Xml4LINQ
                         .Elements("body")  
                         .Elements(aw + "transaction")
                         .Elements("command")
                         .Elements(aw + "documentCommand")
                         .Elements("documentCommandOperand")
                         .Elements(aw + "order")
                    select 
                    new Documento {
                        GLN = ArrayFileName[0],
                        GLNStarfood = ArrayFileName[1],
                        Ctacte = Ctacte,
                        NombreCliente = NombreCliente,
                        Empresa = Empresa,
                        Numero = ArrayFileName[2],
                        Fecha = (DateTime)el.Attribute("creationDate"),

                        TipoPlazoPago = (string)el.Element("paymentTerms").Element("netPayment")
                                                   .Element("timePeriodDue").Attribute("type"),
                        PlazoPago = (int)el.Element("paymentTerms").Element("netPayment")
                                              .Element("timePeriodDue"),

                        FechaVcto = (DateTime)el.Element("movementDate"),
                        UniqueId = (string)el.Element("typedEntityIdentification").Element("entityIdentification")
                                              .Element("uniqueCreatorIdentification"),

                        SalesDepartament = (string)el.Element("salesDepartamentNumber"),
                        TipoOC = (string)el.Element("orderType"),
                        Promocion = (string)el.Element("promotionDealNumber"),
                        NroInternoProveedor = (string)el.Element("internalVendorNumber"),

                        DireccionDespacho = (string)el.Element("shipParty").Element("gln")

                    };
                    foreach (var Encabezado in DoctoImportadora)  
                    {                        
                        //oLog.Add("TRACE", String.Format("Leyendo Encabezado Orden {0} del {1}", Encabezado.Numero, Encabezado.Fecha));
                        DbTable.Add(Encabezado);
                    }
                
                // Caso Distribuidora
                IEnumerable<Documento> DoctoDistribuidora =
                    from el in Xml4LINQ 
                        .Elements(aw + "body")  
                        .Elements(aw + "transaction")
                        .Elements(aw + "command")
                        .Elements(aw + "documentCommand")
                        .Elements(aw + "documentCommandOperand")
                        .Elements(aw + "order")
                    select new Documento {
                        GLN = ArrayFileName[0],
                        GLNStarfood = ArrayFileName[1],
                        Ctacte = Ctacte,
                        NombreCliente = NombreCliente,
                        Empresa = Empresa,
                        Numero = ArrayFileName[2],
                        Fecha = (DateTime)el.Attribute("creationDate"),

                        TipoPlazoPago = (string)el.Element(aw + "paymentTerms").Element(aw + "netPayment")
                                                  .Element(aw + "timePeriodDue").Attribute(aw + "type"),
                        PlazoPago = (int)el.Element(aw + "paymentTerms").Element(aw + "netPayment")
                                              .Element(aw + "timePeriodDue"),

                        FechaVcto = (DateTime)el.Element(aw + "movementDate"),
                        UniqueId = (string)el.Element(aw + "typedEntityIdentification").Element(aw + "entityIdentification")
                                             .Element(aw + "uniqueCreatorIdentification"),

                        SalesDepartament = (string)el.Element(aw + "salesDepartamentNumber"),
                        TipoOC = (string)el.Element(aw + "orderType"),
                        Promocion = (string)el.Element(aw + "promotionDealNumber"),
                        NroInternoProveedor = (string)el.Element(aw + "internalVendorNumber"),

                        DireccionDespacho = (string)el.Element(aw + "shipParty").Element(aw + "gln")

                    };

                    //System.Console.WriteLine(Doctox.Count());

                    
                    foreach (var Encabezado in DoctoDistribuidora)  
                    {                        
                        oLog.Add("TRACE", String.Format("Leyendo Encabezado Orden {0} del {1}", Encabezado.Numero, Encabezado.Fecha));
                        DbTable.Add(Encabezado);
                        //System.Console.WriteLine(Encabezado);
                    }

        // Líneas de Detalle
               //var DoctoD =
                IEnumerable<DocumentoD> DetalleImportadora =
                    from el in Xml4LINQ 
                        .Elements("body")  
                        .Elements(aw + "transaction")
                        .Elements("command")
                        .Elements(aw + "documentCommand")
                        .Elements("documentCommandOperand")
                        .Elements(aw + "order")
                        .Elements("lineItem")
                    select new DocumentoD {
                        Empresa = Empresa,
                        Numero = ArrayFileName[2],

                        Linea = (int)el.Attribute("number"),
                        Precio = (Double)el.Element("netPrice").Element("amount"),
                        ListaPrecio = (string)el.Element("PriceType"),

                        Cantidad = (Double)el.Element("requestedQuantity"),
                        CantidadUnitType = (string)el.Element("requestedQuantity").Attribute("UnitType"),

                        UnidadContenida = (Double)el.Element("containedUnits"),
                        UnidadContenidaUnitType = (string)el.Element("containedUnits").Attribute("UnitType"),

                        Item = (string)el.Element("itemIdentification").Element("gtin"),
                        ItemBuyer = (string)el.Element("itemIdentification").Element("buyerItemNumber"),
                        ItemVendor = (string)el.Element("itemIdentification").Element("vendorItemNumber"),

                        ItemColor = (string)el.Element("itemColor"),
                        ItemSize = (string)el.Element("itemSize"),
                        Descripcion = (string)el.Element("itemDescription").Element("text"),

                        Total = (Double)el.Element("totalAmount").Element("amount")

                    };

                foreach (DocumentoD Registro in DetalleImportadora)  
                {
                    //DocumentoD Detail = new DocumentoD();
                    //Detail.Linea = Registro.Linea;
                    // TODO: Leer  Sispal Reg. Uno a Uno!!!
                    
                    // DataRowCollection Row = GetCodigoProductoSispal(Registro.Item);

                    // if(Row.Count != 0) 
                    // {
                    //     Registro.ItemFlexline = Row[0][0].ToString();
                    //     Registro.Item_Cod_Barras = Row[0][1].ToString();
                    //     Registro.GlosaSisPal = Row[0][2].ToString();

                    // }

                    
                    DbDetail.Add(Registro);
                    
                }
                    

                IEnumerable<DocumentoD> DetalleDistribuidora =
                    from el in Xml4LINQ 
                        .Elements(aw + "body")  
                        .Elements(aw + "transaction")
                        .Elements(aw + "command")
                        .Elements(aw + "documentCommand")
                        .Elements(aw + "documentCommandOperand")
                        .Elements(aw + "order")
                        .Elements(aw + "lineItem")
                    select new DocumentoD {
                        Empresa = Empresa,
                        Numero = ArrayFileName[2],

                        Linea = (int)el.Attribute("number"),
                        Precio = (Double)el.Element(aw + "netPrice").Element(aw + "amount"),
                        ListaPrecio = (string)el.Element(aw + "PriceType"),

                        Cantidad = (Double)el.Element(aw + "requestedQuantity"),
                        CantidadUnitType = (string)el.Element(aw + "requestedQuantity").Attribute("UnitType"),

                        UnidadContenida = (Double)el.Element(aw + "containedUnits"),
                        UnidadContenidaUnitType = (string)el.Element(aw + "containedUnits").Attribute("UnitType"),

                        Item = (string)el.Element(aw + "itemIdentification").Element(aw + "gtin"),
                        ItemBuyer = (string)el.Element(aw + "itemIdentification").Element(aw + "buyerItemNumber"),
                        ItemVendor = (string)el.Element(aw + "itemIdentification").Element(aw + "vendorItemNumber"),

                        ItemColor = (string)el.Element(aw + "itemColor"),
                        ItemSize = (string)el.Element(aw + "itemSize"),
                        Descripcion = (string)el.Element(aw + "itemDescription").Element(aw + "text"),

                        Total = (Double)el.Element(aw + "totalAmount").Element(aw + "amount")

                    };

                foreach (DocumentoD Registro in DetalleDistribuidora)  
                {
                    //DocumentoD Detail = new DocumentoD();
                    //Detail.Linea = Registro.Linea;
                    // TODO: Leer  Sispal Reg. Uno a Uno!!!
                    
                    // DataRowCollection Row = GetCodigoProductoSispal(Registro.Item);

                    // if(Row.Count != 0) 
                    // {
                    //     Registro.ItemFlexline = Row[0][0].ToString();
                    //     Registro.Item_Cod_Barras = Row[0][1].ToString();
                    //     Registro.GlosaSisPal = Row[0][2].ToString();

                    // }

                    
                    DbDetail.Add(Registro);
                    
                }
                
                oLog.Add("TRACE", String.Format("Lectura xml con éxito", ""));

                //sendEmail(Docto, DoctoD);

                return true;
                }
            catch (Exception ex)
            {
                oLog.Add("ERROR",
                    String.Format("Error al obtener DTE's en {0} para periodo {1} {2}",
                    "", "", ex.Message));
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
            // int CantidadFlex = 0;
            // int PrecioFlex = 0;

            string Obs = "";
            Double TotalDocto = 0;
            //int TotalDoctoConvertido = 0;
            
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
                    Detail.Linea, Detail.Item, Detail.ItemFlexline, Detail.GlosaSisPal, Detail.ItemSize,
                    Detail.ItemColor, Detail.CantidadConvertidaFlexline, Detail.PrecioConvertidoFlexline.ToString("#,##0"), Detail.UnidadContenida,
                    Detail.Cantidad.ToString("#,##0.00") , Detail.TotalConvertidoFlexline.ToString("#,##0"), Detail.Observaciones.Replace("\n","<br>")
                    );
                
            }

            DetalleItemDocumento += "        </tbody>";
            DetalleItemDocumento += "</table>";

            Texto = String.Format(Texto, 
                    Cliente, TotalDocto.ToString("#,##0"));

            Mensaje.Body = Texto + (Obs == ""? Normal : Warning) 
                         +"<p>&nbsp;</p>" + EncabezadoPrincipal + "<p>&nbsp;</p>" 
                         + DetalleHead + DetalleItemDocumento 
                         + footerTabla;
                
            smtp.Send(Mensaje);

            oLog.Add("INFO", String.Format("Email enviado con {0} registros informados", "0"));



        }


        // public static DataTable OpenSqlConnection(string Sqltext)
        // {
        //     try {
        //         string connectionString = GetConnectionString();
        //         using (SqlConnection connection = new SqlConnection())
        //         {
        //             connection.ConnectionString = connectionString;

        //             connection.Open();

        //             SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
        //             ComandoSQL.Parameters.AddWithValue("Item","27804647530879");
        //             SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
        //             DataTable dtCommandSQL = new DataTable();
        //             Adapter.Fill(dtCommandSQL);

        //             foreach(var Row in dtCommandSQL.Rows)
        //             {
        //                 System.Console.WriteLine(Row.ToString());
        //             }

        //             return dtCommandSQL;

        //         }
        //     }
        //     catch(Exception ex)
        //     {
        //         oLog.Add("ERROR", String.Format("Error al Leer Datos SQL {0}", ex.Message));
        //         //System.Console.WriteLine("error al  BD ",ex.Message); 
        //         return null;
        //     }
        // }
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
        
        public static DataRowCollection GetCodigoProductoSispal(string CodigoEAN="")
        {
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionSisPal;

                        connection.Open();
                        string SqlText = "Select Isnull(Embalaje_Cod_Barras,'') Embalaje_Cod_Barras, Isnull(Producto,'') Producto, "
                        + "Isnull(Producto_Cod_Barras,'') Producto_Cod_Barras, Isnull(Glosa,'') Glosa " 
                        + "FROM SP_PRODUCTO a " 
                        + "Where " 
                        + "a.Empresa = @Empresa and a.Embalaje_Cod_Barras in ({0}) ";

                        string Items = "";
                        foreach (DocumentoD Row in DbDetail)
                        {
                            Items += "'" + Row.Item + "',";
                        }
                        
                        Items = Items != ""? Left(Items, Items.Length - 1): "''";

                        SqlText = String.Format(SqlText, Items);
                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Empresa", DbTable[0].Empresa);

                        SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
                        DataTable dtCommandSQL = new DataTable();
                        Adapter.Fill(dtCommandSQL);

                        return dtCommandSQL.Rows;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Datos Producto {0} en SisPal {1}", CodigoEAN, ex.Message));
                return null;
            }
        }
         public static DataRowCollection GetCodigoProductoFlexline()
        {
            try {
                using (SqlConnection connection = new SqlConnection())
                    {
                        connection.ConnectionString = Params.StringConexionFlexline;

                        connection.Open();
                         string SqlText = "SELECT a.Producto, Isnull(cta.Listaprecio,0) Listaprecio, Isnull(Lpd.Valor,0) Valor, " 
                        + "Isnull(Lp.Fec_Inicio,'') Fec_Inicio, Isnull(Fec_Final,'') Fec_Final, Isnull(a.Unidad,'') Unidad, " 
                        + "Isnull(a.Vigente,'N') Vigente " 
                        + "FROM Producto a " 
                        + "Left join Ctacte cta on cta.empresa=a.empresa and cta.Ctacte = @Ctacte and cta.Tipoctacte='Cliente' "
                        + "Left Join ListaPrecio Lp on Lp.Empresa=a.Empresa and Lp.LisPrecio = Cta.Listaprecio "
                        + "Left Join ListaPreciod Lpd on Lpd.Empresa=a.Empresa and Lpd.IdLisPrecio=Lp.IdLisPrecio and Lpd.Producto=a.Producto "
                        + "Where " 
                        + "a.Empresa = @Empresa and a.Producto in ({0}) ";

                        string Items = "";
                        foreach (DocumentoD Row in DbDetail)
                        {
                            Items += "'" + Row.ItemFlexline + "',";
                        }
                        
                        Items = Items != ""? Left(Items, Items.Length - 1): "''";

                        SqlText = String.Format(SqlText, Items);
                        SqlCommand ComandoSQL = new SqlCommand(SqlText, connection);
                        ComandoSQL.Parameters.AddWithValue("Ctacte",DbTable[0].Ctacte);
                        ComandoSQL.Parameters.AddWithValue("Empresa",DbTable[0].Empresa);

                        SqlDataAdapter Adapter = new SqlDataAdapter(ComandoSQL);
                        DataTable dtCommandSQL = new DataTable();
                        Adapter.Fill(dtCommandSQL);


                        return dtCommandSQL.Rows;
                    }
            }
            catch(Exception ex)
            {
                oLog.Add("ERROR", String.Format("Error al Leer Datos Producto en SisPal {0}", ex.Message));
                //System.Console.WriteLine("error al  BD ",ex.Message); 
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
 
        public String GLN { get; set; }             // Desde Nombre de archivo
        public String GLNStarfood { get; set; }     // Desde Nombre de archivo
        public String Empresa { get; set; }     // Posición 2 RutSociedades
        public String Ctacte { get; set; }     // Posición 1 CtacteComercioNet2Flex
        public String NombreCliente { get; set; }     // Posición 2 CtacteComercioNet2Flex
        public String Numero { get; set; }          // Desde Nombre de archivo
        public DateTime Fecha { get; set; }           // creationDate
        public String TipoPlazoPago { get; set; }   // paymentTerms/netPayment/timePeriodDue/@type
        public int PlazoPago { get; set; }          // paymentTerms/netPayment/timePeriodDue      
        public DateTime FechaVcto { get; set; }       // movementDate
        public String UniqueId { get; set; }        // typedEntityIdentification/entityIdentification/uniqueCreatorIdentification
        public String SalesDepartament { get; set; }    // salesDepartamentNumber
        public String TipoOC { get; set; }              // orderType
        public String Promocion { get; set; }           // promotionDealNumber
        public String NroInternoProveedor { get; set; } // internalVendorNumber
        public String DireccionDespacho { get; set; }   //shipParty/gln
            

    }

    public class DocumentoD
    {
        public String Empresa { get; set; }                  // DbTable.Empresa
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
        public String ItemFlexline { get; set; }        // Sispal/SP_Producto/Producto
        public String Item_Cod_Barras { get; set; }        // Sispal/SP_Producto/Item_Cod_Barras
        public String GlosaSisPal { get; set; }            // Sispal/SP_Producto/Glosa
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


    }
   


}
