package org.fao.sws.utils;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

public class TariffLineTest {
	
	private static String resourcesDir;
	
	@BeforeClass
	public static void runBuforeClass() throws IOException {
		resourcesDir = new java.io.File( "." ).getCanonicalPath() + "/src/test/resources";
	}
	
	@Test
	public void testReadOne() {
		TariffLineReader.readOne("xx","2010","*",resourcesDir,null,null,null);
	}

	@Test
	public void testReadMany() {
		TariffLineReader.readMany("4 ,1 ,10 ", "2009,2007", "*", 4, resourcesDir, System.out, System.err, System.err);
	}

	@Test
	public void testReadManyFiltered() {
		TariffLineReader.readMany("4-,xx,10", "2010,2011", "27*", 4, resourcesDir, System.out, System.err, System.err);
	}

	
}
