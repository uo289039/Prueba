package bbdd.jdbc1;
import java.io.Console;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Scanner;
import java.util.concurrent.Callable;

import org.hsqldb.jdbcDriver;
import org.hsqldb.types.Type;

import oracle.jdbc.OracleDriver;

public class Program {
	
	private static String USERNAME = "UO289039";
	private static String PASSWORD = "passUO289039";
	private static String CONNECTION_STRING = "jdbc:oracle:thin:@156.35.94.98:3389:desa19";
	//private static Connection conn = DriverManager.getConnection(url,USERNAME, PASSWORD);

	public static void main(String[] args) {
		//Ejemplos para leer por teclado
//		System.out.println("Leer un entero por teclado");	
//		int entero = ReadInt();
//		System.out.println("Leer una cadena por teclado");	
//		String cadena = ReadString();
		
		
		try {
//			exercise1_1();
//			exercise1_2();
//			exercise2();
//			exercise3();
//			exercise4();
//			exercise6_1();
//			exercise5_2();
//			exercise9();
			exercise10();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
		1.	Crear un metodo en Java que muestre por pantalla los resultados de las consultas 21 y 32 de la Practica SQL2. 
		1.1. (21) Obtener el nombre y el apellido de los clientes que han adquirido un coche en un concesionario de Madrid, el cual dispone de coches del modelo gti.
	 */
	public static void exercise1_1() throws SQLException{
		System.out.println("#########EJERCICIO 1_1#########");
		Connection con =getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="select nombre, apellido from clientes c, "
				+ "ventas v, coches ch, concesionarios cn where c.dni=v.dni "
				+ "and v.codcoche=ch.codcoche and "
				+ "ch.modelo='gti' and cn.cifc=v.cifc and cn.ciudadc='madrid'";
		query.append(sentencia);
		Statement st=con.createStatement();
		ResultSet rs = st.executeQuery(query.toString());
		showResults(rs);
		rs.close();
		st.close();
		con.close();
		
	}
	
	/* 
		1.2. (32) Obtener un listado de los concesionarios cuyo promedio de coches supera a la cantidad promedio de todos los concesionarios. 
	*/
	public static void exercise1_2() throws SQLException{
		System.out.println("#########EJERCICIO 1_2#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="select co.nombrec, co.ciudadc from concesionarios co, distribucion d "
				+ "where co.cifc = d.cifc "
				+ "group by co.nombrec, co.ciudadc "
				+ "having sum(d.cantidad) > (select avg(total) from "
				+ "(select sum(cantidad) as total from distribucion group by cifc))";
		query.append(sentencia);
		Statement st=con.createStatement();
		ResultSet rs=st.executeQuery(query.toString());
		showResults(rs);
		rs.close();
		st.close();
		con.close();
	}
	
	public static void exercise1_3() throws SQLException{
		System.out.println("#########EJERCICIO 1_1#########");
		Connection con =getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="select nombre, apellido from clientes c, "
				+ "ventas v, coches ch, concesionarios cn where c.dni=v.dni "
				+ "and v.codcoche=ch.codcoche and "
				+ "ch.modelo='lexus' and cn.cifc=v.cifc and cn.ciudadc='barcelona'";
		query.append(sentencia);
		Statement st=con.createStatement();
		ResultSet rs = st.executeQuery(query.toString());
		showResults(rs);
		rs.close();
		st.close();
		con.close();
		
	}
	
	/*
		2. Crear un metodo en Java que muestre por pantalla el resultado de la consulta 6 de la Practica SQL2 de forma el color de la busqueda sea introducido por el usuario.
			(6) Obtener el nombre de las marcas de las que se han vendido coches de un color introducido por el usuario.
	*/
	public static void exercise2() throws SQLException {
		System.out.println("#########EJERCICIO 2#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="select nombrem from marcas m, marco mc, coches c, ventas v "
				+ "where m.cifm=mc.cifm and mc.codcoche=c.codcoche and "
				+ "v.codcoche=c.codcoche and v.color=?";
		query.append(sentencia);
		PreparedStatement pst=con.prepareStatement(query.toString());
		System.out.println("Introduzca el color buscado: ");
		String color=ReadString();
		pst.setString(1, color);
		ResultSet rs=pst.executeQuery();
		showResults(rs);
		rs.close();
		pst.close();
		con.close();
		
	}
	
	/*
		3.	Crear un metodo en Java para ejecutar la consulta 27 de la Practica SQL2 de forma que los limites la cantidad de coches sean introducidos por el usuario. 
			(27) Obtener el cifc de los concesionarios que disponen de una cantidad de coches comprendida entre dos cantidades introducidas por el usuario, ambas inclusive.

	*/
	public static void exercise3() throws SQLException {
		System.out.println("#########EJERCICIO 3#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="select distinct c.cifc from concesionarios c, "
				+ "distribucion d where c.cifc=d.cifc and d.cantidad>=? and "
				+ "d.cantidad<=?";
		query.append(sentencia);
		PreparedStatement pst=con.prepareStatement(query.toString());
		System.out.println("Intrduzca el minimo de coches buscados:");
		String l1=ReadString();
		System.out.println("Intrduzca el maximo de coches buscados:");
		String l2=ReadString();
		pst.setString(1, l1);
		pst.setString(2, l2);
		ResultSet rs=pst.executeQuery();
		showResults(rs);
		rs.close();
		pst.close();
		con.close();
	}
	
	/*
		4.	Crear un metodo en Java para ejecutar la consulta 24 de la Practica SQL2 de forma que tanto la ciudad del concesionario como el color sean introducidos por el usuario. 
			(24) Obtener los nombres de los clientes que no han comprado coches de un color introducido por el usuario en concesionarios de una ciudad introducida por el usuario.

	*/
	public static void exercise4() throws SQLException {
		System.out.println("#########EJERCICIO 4#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="select distinct nombre from clientes c,ventas v where "
				+ "c.dni=v.dni and c.dni not in(select v.dni from ventas v, "
				+ "concesionarios cn "
				+ "where c.dni=v.dni and v.color=? and cn.ciudadc=?)";
		query.append(sentencia);
		PreparedStatement pst=con.prepareStatement(query.toString());
		System.out.println("Introduce el color betado: ");
		String color=ReadString();
		System.out.println("Introduce la ciudad del concesionario: ");
		String ciudad=ReadString();
		pst.setString(1, color);
		pst.setString(2, ciudad);
		ResultSet rs=pst.executeQuery();
		showResults(rs);
		rs.close();
		pst.close();
		con.close();
	}
	
	/*
		5.	Crear un metodo en Java que haciendo uso de la instruccion SQL adecuada: 
		5.1. Introduzca datos en la tabla coches cuyos datos son introducidos por el usuario.

	*/
	public static void exercise5_1() throws SQLException {
		System.out.println("#########EJERCICIO 5_1#########");
		Connection con =getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="insert into coches (codcoche,nombrech,modelo) values(?,?,?)";
		query.append(sentencia);
		PreparedStatement pst=con.prepareStatement(query.toString());
		System.out.println("Introduce el codigo del coche, numerico:");
		int cod=ReadInt();
		pst.setInt(1, cod);
		System.out.println("Introduce el nombre del coche:");
		String nom=ReadString();
		pst.setString(2, nom);
		System.out.println("Introduce el modelo del coche:");
		String mod=ReadString();
		pst.setString(3, mod);
		if(pst.executeUpdate()==1)
			System.out.println("Insercion realizada correctamente");
		else
			System.out.println("No se podido realizar la insercion");
		pst.close();
		con.close();
	}
	
	/*
		5.2. Borre un determinado coche cuyo codigo es introducido por el usuario. 
	*/
	public static void exercise5_2() throws SQLException {
		System.out.println("#########EJERCICIO 5_2#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="delete from coches where codcoche=?";
		query.append(sentencia);
		PreparedStatement pst=con.prepareStatement(query.toString());
		System.out.println("Introduce el codigo del coche a borrar:");
		int cod=ReadInt();
		pst.setInt(1, cod);
		if(pst.executeUpdate()==1)
			System.out.println("Coche borrado");
		else
			System.out.println("No se ha borrado el coche");
		pst.close();
		con.close();
	}
	
	/*	 
		5.3. Actualice el nombre y el modelo para un determinado coche cuyo codigo es introducido por el usuario.
	*/
	public static void exercise5_3() throws SQLException {		
		System.out.println("#########EJERCICIO 5_3#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="update coches set nombrech=?, modelo=? where codcoche=?";
		query.append(sentencia);
		PreparedStatement pst=con.prepareStatement(query.toString());
		System.out.println("Introduzca el nuevo nombre del coche:");
		String nombre=ReadString();
		pst.setString(1, nombre);
		System.out.println("Introduzca el nuevo modelo del coche:");
		String modelo=ReadString();
		pst.setString(2, modelo);
		System.out.println("Introduzca el codigo del coche:");
		int cod=ReadInt();
		pst.setInt(3, cod);
		
		if(pst.executeUpdate()==1)
			System.out.println("Actualización realizada");
		else
			System.out.println("No se ha podido actualizar");
		pst.close();
		con.close();
		
		
		
	}
	
	/*
		6. Invocar la funcion y el procedimiento del ejercicio 10 de la practica PL1 desde una aplicacion Java. 
			(10) Realizar un procedimiento y una funcion que dado un codigo de concesionario devuelve el numero ventas que se han realizado en el mismo.
		6.1. Funcion
	*/
	public static void exercise6_1() throws SQLException {		
		if(!CONNECTION_STRING.contains("oracle"))return ;
		System.out.println("#########EJERCICIO 6_1#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="{?=call FUNCION10(?)}";
		query.append(sentencia);
		CallableStatement cst=con.prepareCall(query.toString());
		System.out.println("Introduce el código del concesionario:");
		String cifc=ReadString();
		cst.setString(2, cifc);
		cst.registerOutParameter(1, Types.INTEGER,0);
		cst.execute();
		System.out.println("Vendidos:"+cst.getString(1));
		cst.close();
		con.close();
		
}
	
	/*	
		6.2. Procedimiento
	*/
	public static void exercise6_2() throws SQLException {		
		if(!CONNECTION_STRING.contains("oracle"))return;
		System.out.println("#########EJERCICIO 6_2#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="{call PROCEDIMIENTO10_SQL(?,?)}";
		query.append(sentencia);
		CallableStatement cst=con.prepareCall(query.toString());
		System.out.println("Introduce el código del concesionario:");
		String cifc=ReadString();
		cst.setString(1, cifc);
		cst.registerOutParameter(2,Types.INTEGER, 0);
		cst.execute();
		System.out.println("Ventas:"+cst.getString(2));
		cst.close();
		con.close();
	}
	
	/*
		7. Invocar la funcion y el procedimiento del ejercicio 11 de la Practica PL1 desde una aplicacion Java. 
			(11) Realizar un procedimiento y una funcion que dada una ciudad que se le pasa como parametro devuelve el numero de clientes de dicha ciudad.
		7.1. Funcion

	*/
	public static void exercise7_1() throws SQLException {		
		if(!CONNECTION_STRING.contains("oracle")) return;
		System.out.println("#########EJERCICIO 7_1#########");
		Connection con=getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="{? = call FUNCION11(?)}";
		query.append(sentencia);
		
		System.out.println("Introduzca la ciudad del cliente:");
		String ciudad=ReadString();
		
		CallableStatement cst=con.prepareCall(query.toString());
		
		cst.registerOutParameter(1,Types.INTEGER,0); 
		cst.setString(2, ciudad);
		cst.execute();
		System.out.println("Resultado: "+cst.getInt(1));
		cst.close();
		con.close();
	}	
	
	/*
		7.2. Procedimiento
	*/
	public static void exercise7_2() throws SQLException {		
		if(!CONNECTION_STRING.contains("oracle")) return ;
		System.out.println("#########EJERCICIO 7_2#########");
		Connection con =getConnection();
		StringBuilder query=new StringBuilder();
		String sentencia="{call PROCEDIMIENTO11(?,?)}";
		query.append(sentencia);
		
		System.out.println("Introduce el nombre de la ciudad en cuestión:");
		String ciudad=ReadString();
		
		CallableStatement cst=con.prepareCall(query.toString());
		
		cst.setString(1, ciudad);
		cst.registerOutParameter(2,Types.INTEGER,0);
		cst.execute();
		
		System.out.println("Resultado: "+cst.getInt(2));
		
		cst.close();
		con.close();
	
		
	}
	
    /*
     	8. Crear un metodo en Java que imprima por pantalla los coches que han sido adquiridos por cada cliente.
     	Ademas, debera imprimirse para cada cliente el numero de coches que ha comprado y el numero de
     	concesionarios en los que ha comprado. Aquellos clientes que no han adquirido ningun coche no
		deben aparecer en el listado.
    */
	public static void exercise8() throws SQLException {		
		System.out.println("#########EJERCICIO 8#########");
		Connection con=getConnection();
		StringBuilder queryClientes=new StringBuilder();
		String sentenciaClientes="select distinct dni, nombre, apellido from clientes";
		queryClientes.append(sentenciaClientes);
		Statement stClientes=con.createStatement();
		ResultSet rsClientes=stClientes.executeQuery(queryClientes.toString());
		
		StringBuilder queryInfoC=new StringBuilder();
		String sentenciaInfoC="select count(*) as nch, count(distinct cifc) as ncn from "
				+ "ventas where dni=?";
		queryInfoC.append(sentenciaInfoC);
		PreparedStatement pstInfoC=con.prepareStatement(queryInfoC.toString());
		
		StringBuilder queryInfoCoche=new StringBuilder();
		String sentenciaInfoCoche="select c.codcoche,c.nombrech,c.modelo,v.color from"
				+ " coches c, ventas v where v.codcoche = c.codcoche and v.dni=?";
		queryInfoCoche.append(sentenciaInfoCoche);
		PreparedStatement pstInfoCoche=con.prepareStatement(queryInfoCoche.toString());
		
		while(rsClientes.next()) {
			pstInfoC.setString(1, rsClientes.getString("dni"));
			ResultSet rs=pstInfoC.executeQuery();
			rs.next();
			System.out.println("-Cliente: "+rsClientes.getString("nombre")+" "
					+rsClientes.getString("apellido")+" "+rs.getString("nch")+" "
					+rs.getString("ncn"));
			pstInfoCoche.setString(1, rsClientes.getString("dni"));
			rs=pstInfoCoche.executeQuery();
			while(rs.next()) {
				System.out.println("---> Coche: "+rs.getString("codcoche")+" "
						+rs.getString("nombrech")+" "+rs.getString("modelo")+" "
						+rs.getString("color"));
			}
			rs.close();
		}
		pstInfoCoche.close();
		pstInfoC.close();
		rsClientes.close();
		stClientes.close();
		con.close();
	}
	
	
	public static void exercise9() throws SQLException {		
		System.out.println("#########EJERCICIO 9#########");
		Connection con=getConnection();
		StringBuilder queryCineRecaudacionT=new StringBuilder();
		String sentenciaCineRecaudacionT="select c.codcine,sum(precio) as income from"
				+ " cines c, salas s, entradas e where c.codcine=s.codcine and "
				+ "s.codsala=e.codsala and localidad=? group by c.codcine";
		queryCineRecaudacionT.append(sentenciaCineRecaudacionT);
		System.out.println("Introduce la localidad del cine deseado:");
		String localidad=ReadString();
		
		PreparedStatement pstCineRecaudacionT=con.prepareStatement(queryCineRecaudacionT.toString());
		pstCineRecaudacionT.setString(1, localidad);
		ResultSet rsCineRecaudacionT=pstCineRecaudacionT.executeQuery();
		
		
		StringBuilder queryCineRecaudacionL=new StringBuilder();
		String sentenciaCineRecaudacionL="select p.codpelicula, p.titulo, sum(precio)"
				+ " as income from peliculas p, entradas e, salas s where s.codcine=?"
				+ " and p.codpelicula=e.codpelicula and e.codsala=s.codsala "
				+ "GROUP BY p.codpelicula, p.titulo";
		
		queryCineRecaudacionL.append(sentenciaCineRecaudacionL);
		PreparedStatement pst=con.prepareStatement(queryCineRecaudacionL.toString());
		
		while(rsCineRecaudacionT.next()) {
			pst.setString(1, rsCineRecaudacionT.getString("codcine"));
			ResultSet rs=pst.executeQuery();
			//rs.next();
			System.out.println("Cine: "+rsCineRecaudacionT.getString("codcine")+" - "
					+ rsCineRecaudacionT.getString("income"));
			rs=pst.executeQuery();

			while(rs.next()) {
				System.out.println("\t----> Pelicula: "+ rs.getString("codpelicula")
				+" "+rs.getString("titulo")+" "+rs.getString("income"));
			}
			rs.close();
		}
		pst.close();
		rsCineRecaudacionT.close();
		pstCineRecaudacionT.close();
		con.close();
	}

	
	public static void exercise10() throws SQLException {
		System.out.println("#########EJERCICIO 10#########");
		Connection con=getConnection();
		StringBuilder queryTitulo=new StringBuilder();
		String sentenciaTitulo="select codpelicula,titulo from peliculas";
		queryTitulo.append(sentenciaTitulo);
		Statement stTitulo=con.createStatement();
		ResultSet rsTitulo=stTitulo.executeQuery(queryTitulo.toString());
		
		StringBuilder queryCine=new StringBuilder();
		String sentenciaCine="select distinct c.codcine from cines c, salas s, "
				+ "proyectan p where s.codcine = c.codcine and s.codsala = p.codsala"
				+ " and p.codpelicula=?";
		queryCine.append(sentenciaCine);
		PreparedStatement pstCine=con.prepareStatement(queryCine.toString());
		
		StringBuilder queryInfo=new StringBuilder();
		String sentenciaInfo="select distinct s.codsala,p.sesion, "
				+ "sum(entradasvendidas) as ev from salas s, proyectan p where "
				+ "s.codsala = p.codsala and s.codcine=? and p.codpelicula=? "
				+ "group by s.codsala,p.sesion";
		queryInfo.append(sentenciaInfo);
		PreparedStatement pstInfo=con.prepareStatement(queryInfo.toString());
		
		while(rsTitulo.next()) {
			System.out.println(rsTitulo.getString("titulo"));
			pstCine.setString(1,  rsTitulo.getString("codpelicula"));
			ResultSet rsCine=pstCine.executeQuery();
			if(!rsCine.next())
				System.out.println("La pelicula no se proyecta en ningún cine");
			else {
				do {
					System.out.println(" "+rsCine.getString("codcine"));
					pstInfo.setString(1, rsCine.getString("codcine"));
					pstInfo.setString(2, rsTitulo.getString("codpelicula"));
					ResultSet rsInfo=pstInfo.executeQuery();
					while(rsInfo.next()) {
						System.out.println("  "+rsInfo.getString(1)+" "+
					rsInfo.getString(2)+" "+rsInfo.getString(3));
					}
					rsInfo.close();
				}while(rsCine.next());
			}
			rsCine.close();
		}
		pstInfo.close();
		pstCine.close();
		rsTitulo.close();
		stTitulo.close();
		con.close();
		
	}
	
	
	private static Connection getConnection() throws SQLException{
		if(DriverManager.getDriver(CONNECTION_STRING)==null)
			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
		else
			DriverManager.registerDriver(new org.hsqldb.jdbc.JDBCDriver());
		return DriverManager.getConnection(CONNECTION_STRING,USERNAME,PASSWORD);
	}
		
	
	
	private static void showResults(ResultSet rs) throws SQLException{
		int columnCount=rs.getMetaData().getColumnCount();
		StringBuilder headers=new StringBuilder();
		for(int i=1;i<columnCount;i++) 
			headers.append(rs.getMetaData().getColumnName(i)+"\t");
		headers.append(rs.getMetaData().getColumnName(columnCount));
		
		System.out.println(headers.toString());
		
		StringBuilder result=null;
		
		while(rs.next()) {
			result=new StringBuilder();
			for(int i=1;i<columnCount;i++)
				result.append(rs.getObject(i)+"\t");
			result.append(rs.getObject(columnCount));
			System.out.println(result.toString());
		}
		
		if(result==null)
			System.out.println("No se encontraron datos");
	}
	
	
	
	
	@SuppressWarnings("resource")
	private static String ReadString(){
		return new Scanner(System.in).nextLine();		
	}
	
	@SuppressWarnings("resource")
	private static int ReadInt(){
		return new Scanner(System.in).nextInt();			
	}	
	
	
	
}
