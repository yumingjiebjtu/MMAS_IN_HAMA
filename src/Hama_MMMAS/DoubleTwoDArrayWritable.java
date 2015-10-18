/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hama_MMMAS;

/**
 *
 * @author mjh
 */
import org.apache.hadoop.io.*;
public class DoubleTwoDArrayWritable extends TwoDArrayWritable
    {
        public DoubleTwoDArrayWritable()
        {
                super(DoubleWritable.class);
        }
         public DoubleTwoDArrayWritable(DoubleWritable[][] values)
        {
                super(DoubleWritable.class,values);
        }
                 
}
