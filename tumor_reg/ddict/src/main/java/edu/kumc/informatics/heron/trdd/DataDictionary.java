/**
 * 
 */
package edu.kumc.informatics.heron.trdd;

import java.io.File;
import java.io.IOException;

import com.healthmarketscience.jackcess.Database;

/**
 * @author dconnolly
 *
 */
public class DataDictionary {

        /**
         * @param args
         * @throws IOException 
         */
        public static void main(String[] args) throws IOException {
                File f = new File("/home/dconnolly/bmidev/tumor_reg/naaccr12_1.mdb");
                Database db = Database.open(f);
                System.out.println(db.getFileFormat());
                                
//                                .getTable("MyTable").display());

        }

}
