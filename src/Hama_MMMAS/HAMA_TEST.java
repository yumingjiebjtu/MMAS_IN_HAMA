/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hama_MMMAS;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
//import org.apache.hama.bsp.message.compress.SnappyCompressor;
import org.apache.hama.bsp.sync.SyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * @author yu
 */
public class HAMA_TEST
        extends
        BSP<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> {
    
    public static final Logger log =LoggerFactory
      .getLogger(HAMA_TEST.class);
     
    public static final String PBEST = "mmas.initial.theta";
    public static final String ALPHA = "mmas.alpha";
    public static final String BETA = "mmas.beta";
    public static final String ROU = "mmas.rou";
    public static final String ITERATIONS_MMAS = "mmas.iterations.mmas";
    public static final String ANT_COUNT = "mmas.ant.count";
    public static final String CITYNUM = "mmas.city.num";
    public static final String DBRATE = "mmas.dbrate";
    
    
    
    
    //首先坐标以变量形式定义，日后再将从文件读取导入
    public double  x_Ary[]=
        {
            37,49,52,20,40,21,17,31,52,51,
            42,31,5,12,36,52,27,17,13,57,
            62,42,16,8,7,27,30,43,58,58,
            37,38,46,61,62,63,32,45,59,5,
            10,21,5,30,39,32,25,25,48,56,
            30
        };
    public double y_Ary[]=
        {
            52,49,64,26,30,47,63,62,33,21,
            41,32,25,42,16,41,23,33,13,58,
            42,57,57,52,38,68,48,67,48,27,
            69,46,10,33,63,69,22,35,15,6,
            17,10,64,15,10,39,32,55,28,37,
            40
        };
    
    
    
    
    
    
    
    private  float Alpha; 
    private  float Beta;
    private  float Rou; //信息素残留系数，(1-ROU)就是信息素的挥发速度
    private  float Pbest; //蚂蚁一次搜索找到最优路径的概率
    private  int AntCount;//蚂蚁的数量
    private  int Iterations;//蚁群迭代次数
    private  int maxIterations;//外层循环迭代次数   
    private  float dbRate;//最大信息素与最小信息素的比值
    
    
    
    private int CityNum;
    private boolean master;
//    private String masterTask;
    
    private double[][] The_Distance;
    private double[][] The_Distance_Beta;
    private double[][] The_Matrix ;
    private double[][] The_Matrix_Possible;
    
    
    private MMAS mmas;
    
    private static final Path TMP_OUTPUT = new Path("/tmp/RealTime_"+ System.currentTimeMillis());
    
    @Override
    public void setup(
            BSPPeer<NullWritable, NullWritable, Text,DoubleWritable, DoubleTwoDArrayWritable> peer)
            throws IOException, SyncException, InterruptedException {
//          Choose one as a master
            master = peer.getPeerIndex() == peer.getNumPeers() / 2;
//            masterTask = peer.getPeerName(peer.getNumPeers() / 2);
//            master = peer.getPeerName().equals(masterTask);
            
            int a = x_Ary.length;            
            CityNum =  peer.getConfiguration().getInt(CITYNUM, a);
            Pbest = peer.getConfiguration().getFloat(PBEST, 0.05f);
            //对Pbest开CITYNUM次方
            double dbTemp=Math.exp(Math.log(Pbest)/(double)CityNum); 
            double dbFz=2.0*(1.0-dbTemp);
            double dbFm=((double)CityNum-2.0)*dbTemp;
            float b=(float) (dbFz/dbFm);            
            dbRate = peer.getConfiguration().getFloat(DBRATE,b);            
                        
            Alpha = peer.getConfiguration().getFloat(ALPHA, 1.0f);
            Beta = peer.getConfiguration().getFloat(BETA, 2.0f);
            Rou = peer.getConfiguration().getFloat(ROU, 0.98f);

            AntCount = peer.getConfiguration().getInt(ANT_COUNT, 20);
            Iterations = peer.getConfiguration().getInt(ITERATIONS_MMAS, 100);           
            maxIterations = peer.getConfiguration().getInt("max.iterations", 5);
            

            The_Matrix_Possible = new double[CityNum][CityNum];

            The_Distance_Beta = new double[CityNum][CityNum];
            
    }

    @Override
    public void bsp(
            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer)
            throws IOException, SyncException, InterruptedException {
            
        
            //superstep -1 :搞定The_Distance与The_Distance_Beta
//            getInitialDistanceALPHA(peer);
            
            getInitialDistanceBETA(peer);
            
            // superstep 0 :get initial matrix
            getInitialMatrix(peer);
            
            //Need a "peer.sync()"here? maybe no
            
            int iterations = 0;
            while(true){
                if(iterations>maxIterations)
                    break;
            //superstep 1 : calculate the Matrix in parallel    
                double[][] localMatrix;
                localMatrix = calculateLocalMatrix();
                
            //The Matrix is sent and aggregated by each
                broadcastMatrix(peer, localMatrix);
                peer.sync();
                
            //superstep 2 :aggregate Matrix calculation    
                double[][] newMatrix = new double[localMatrix.length][localMatrix[0].length]; 
                newMatrix = aggregateTheMatrix(peer, newMatrix);
                
            //update The_Matrix    
                updateTheta(peer, newMatrix);
                
                if (log.isDebugEnabled()) {
                    log.debug("{}: new matrix is {}", new Object[]{peer.getPeerName(), newMatrix});
                }
                if (master) {
                    peer.write(new Text("Now The Best IS "), new DoubleWritable(mmas.GlobalBestAnt.TourLength));
                }
                peer.sync();
                
                iterations++;
            }
    }
    
    //still need to finish this part
    
    @Override
    public void cleanup(
            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer)
            throws IOException{
        //still need to finish this part
        
        //here,write(k2,v2),k2&v2 means key of output and value of output,they should be same with the third & forth 
            
        //master writes down the final outputIOException, SyncException, InterruptedException{
        //still need to finish this part
        
        //here,write(k2,v2),k2&v2 means key of output and value of output,they should be same with the third & forth 
            
        //master writes down the final output
            if (master) {
                peer.write(new Text("The Final is "), new DoubleWritable(mmas.GlobalBestAnt.TourLength));
//              if (log.isInfoEnabled()) {
//                log.info("{}:computation finished with cost {} and theta {}", new Object[]{peer.getPeerName(), cost, theta});
              }
        //    }
    }
    
    private double[][] handletheMessage(
            DoubleTwoDArrayWritable In) throws IOException {
            int col,row;
            DoubleWritable mid;
            Writable[][] temp;
            temp = In.get();
            col = temp.length;
            row = temp[0].length;
            double[][] Out = new double[col][row];
            for (int k1 = 0; k1 < col; k1++) {
                for(int j1=0;j1< row;j1++){
                    mid = (DoubleWritable)temp[k1][j1];
                    Out[k1][j1] = mid.get();
                }
            }
            return Out;
    }
    
    private void broadcastMatrix(
            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer,
            double[][] Matrix) throws IOException {
            int col,row;
            col = Matrix.length;
            row = Matrix[0].length;
            DoubleTwoDArrayWritable Send_Matrix = new DoubleTwoDArrayWritable(); 
            DoubleWritable[][] Recieve_Matrix = new DoubleWritable[col][row];
            for (int k1 = 0; k1 < col; k1++) {
                for(int j1=0;j1< row;j1++){
                    Recieve_Matrix[k1][j1] = new DoubleWritable(Matrix[k1][j1]);
                }
            }
            Send_Matrix.set(Recieve_Matrix);
            for (String peerName : peer.getAllPeerNames()) {
                if (!peerName.equals(peer.getPeerName())) { // avoid sending to oneself
                    peer.send(peerName, Send_Matrix);
                }    
            }
    }
    
//    private void sendtoMaster(
//            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer,
//            double[][] Matrix) throws IOException {
//            int col,row;
//            col = Matrix.length;
//            row = Matrix[0].length;
//            DoubleTwoDArrayWritable Send_Matrix = new DoubleTwoDArrayWritable(); 
//            DoubleWritable[][] Recieve_Matrix = new DoubleWritable[col][row];
//            for (int k1 = 0; k1 < col; k1++) {
//                for(int j1=0;j1< row;j1++){
//                    Recieve_Matrix[k1][j1] = new DoubleWritable(Matrix[k1][j1]);
//                }
//            }
//            Send_Matrix.set(Recieve_Matrix);
//            peer.send(masterTask, Send_Matrix);
//             
//    }
    
    private double[][] aggregateTheMatrix(
            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer,
            double[][] thematrix) throws IOException {
            double[][] newMatrix,tempMatrix ;
            newMatrix = thematrix.clone();
            DoubleTwoDArrayWritable temp;
            while ((temp = peer.getCurrentMessage()) != null) {
                tempMatrix = handletheMessage(temp);
                for(int k1 = 0; k1 < thematrix.length; k1++) {
                    for(int j1=0;j1< thematrix[0].length;j1++){
                        newMatrix[k1][j1] += tempMatrix[k1][j1];
                    }    
                }
            }
            return newMatrix;
    }
  
//    private double[][] calculateLocalMatrix(
//            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer)
//            throws IOException {
    private double[][] calculateLocalMatrix()
            throws IOException {
        //It is a temp one, any bsp method wasn't used.
//    double localCost = 0d;
//
//    // read an item
//    KeyValuePair<VectorWritable, DoubleWritable> kvp;
//    while ((kvp = peer.readNext()) != null) {
//      // calculate cost for given input
//      double y = kvp.getValue().get();
//      DoubleVector x = kvp.getKey().getVector();
//      double costForX = regressionModel.calculateCostForItem(x, y, m, theta).doubleValue();
//
//      // adds to local cost
//      localCost += costForX;
        
            for (int i=0;i<CityNum;i++)
            {
                for (int j=0;j<CityNum;j++)
                {
                    The_Matrix_Possible[i][j]=Math.pow(The_Matrix[i][j],Alpha)*The_Distance_Beta[i][j];
                }
            }
            double[][] localMatrix = new double[CityNum][CityNum];
            mmas = new MMAS(CityNum,Alpha,Beta,Rou,Pbest,AntCount,Iterations,
                    dbRate,The_Distance,The_Distance_Beta,The_Matrix,The_Matrix_Possible);
            mmas.Initalize();
            mmas.Solve();            
            localMatrix = mmas.Pheromone;                                   
            return localMatrix;
    }
    //此处用于生成初始信息素矩阵
    private void getInitialMatrix(
          BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer)
          throws IOException, SyncException, InterruptedException {
        if (The_Matrix == null) {
            if (master) {
                The_Matrix = new double[CityNum][CityNum];
                Ant ant = new Ant(CityNum,The_Distance,The_Matrix_Possible);
                //贪婪算法生成最初始解
                ant.Init();
                ant.GreedSearchEnable = true;
                ant.Search();
        
                double Imax=1.0/(ant.TourLength*(1.0-Rou));

                //初始化环境信息素
                for (int i=0;i<CityNum;i++)
                {
                        for (int j=0;j<CityNum;j++)
                        {
                                The_Matrix[i][j]=Imax;
//                                The_Matrix_Possible[i][j]=Math.pow(The_Matrix[i][j],Alpha)*The_Distance_Beta[i][j];
                        }
                }
                broadcastMatrix(peer, The_Matrix);
                if (log.isDebugEnabled()) {
                    log.debug("{}: sending matrix", peer.getPeerName());
            }
            peer.sync();
            }
            else {
                if (log.isDebugEnabled()) {
                    log.debug("{}: getting matrix", peer.getPeerName());
                }
            peer.sync();
            DoubleTwoDArrayWritable doubleTwoD = peer.getCurrentMessage();
            The_Matrix = handletheMessage(doubleTwoD);
            }
        }
    }

//      getInitialDistanceALPHA 为主机计算The_Distance，然后传输至从机，
//      从机收到后，改变从机的The_Distance,再依据The_Distance生成The_Distance_Beta
    private void getInitialDistanceALPHA(
          BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer)
          throws IOException, SyncException, InterruptedException {
        if (The_Distance == null) {
            if (master) {
                The_Distance = new double[CityNum][CityNum];
                The_Distance_Beta = new double[CityNum][CityNum];
                double dbTemp=0.0;
                for (int i=0;i<CityNum;i++)
                {
                    for (int j=0;j<CityNum;j++)
                    {
                        dbTemp=(x_Ary[i]-x_Ary[j])*(x_Ary[i]-x_Ary[j])+(y_Ary[i]-y_Ary[j])*(y_Ary[i]-y_Ary[j]);
                        dbTemp=Math.pow(dbTemp,0.5);

//                      The_Distance[i][j]=(double)((int)(dbTemp+0.5));
                        The_Distance[i][j]=dbTemp;
                        The_Distance_Beta[i][j]=Math.pow(1.0/The_Distance[i][j],(double)Beta);                        
                    }
                }
                broadcastMatrix(peer,The_Distance);
                if (log.isDebugEnabled()) {
                    log.debug("{}: sending matrix", peer.getPeerName());
                }
                peer.sync();
            }
            else
            {
                if (log.isDebugEnabled()) {
                    log.debug("{}: getting matrix", peer.getPeerName());
                }
                peer.sync();
                DoubleTwoDArrayWritable doubleTwoD = peer.getCurrentMessage();
                The_Distance = handletheMessage(doubleTwoD);
                for (int i=0;i<CityNum;i++)
                {
                    for (int j=0;j<CityNum;j++)
                    {
                        The_Distance_Beta[i][j]=Math.pow(1.0/The_Distance[i][j],(double)Beta);                        
                    }
                }
            }
        }
    }
    //getInitialDistanceBETA为主机从机均依据横纵坐标去计算
    //The_Distance与The_Distance_Beta
    private void getInitialDistanceBETA(
          BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer)
          throws IOException, SyncException, InterruptedException {
        if (The_Distance == null) {
                The_Distance = new double[CityNum][CityNum];

                double dbTemp=0.0;
                for (int i=0;i<CityNum;i++)
                {
                    for (int j=0;j<CityNum;j++)
                    {
                        dbTemp=(x_Ary[i]-x_Ary[j])*(x_Ary[i]-x_Ary[j])+(y_Ary[i]-y_Ary[j])*(y_Ary[i]-y_Ary[j]);
                        dbTemp=Math.pow(dbTemp,0.5);

                      The_Distance[i][j]=(double)((int)(dbTemp+0.5));
//                        The_Distance[i][j]=dbTemp;
                        The_Distance_Beta[i][j]=Math.pow(1.0/The_Distance[i][j],(double)Beta);                        
                    }
                }

            }
        }   
    
    
    
    
    private void updateTheta(
            BSPPeer<NullWritable, NullWritable, Text, DoubleWritable, DoubleTwoDArrayWritable> peer,
            double[][] thematrix) throws IOException {
            double[][] temp = new double [thematrix.length][thematrix[0].length];
            for(int k1 = 0; k1 < thematrix.length; k1++) {
                for(int j1=0;j1< thematrix[0].length;j1++){
//                    temp[k1][j1] = thematrix[k1][j1]/peer.getNumPeers() ;
                    temp[k1][j1] = thematrix[k1][j1]/(peer.getNumPeers()-1) ;
                }
            }            
            The_Matrix = temp;
    }
    
    static void printOutput(HamaConfiguration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(TMP_OUTPUT);
            for (FileStatus file : files) {
                if (file.getLen() > 0) {
                    try (FSDataInputStream in = fs.open(file.getPath())) {
                        IOUtils.copyBytes(in, System.out, conf, false);
                    }
                    break;
                }
            }

    fs.delete(TMP_OUTPUT, true);
  }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        HamaConfiguration conf = new HamaConfiguration();

        conf.set("max.iterations", "");

        BSPJob job = new BSPJob(conf);
        // set the BSP class which shall be executed
        job.setBspClass(HAMA_TEST.class);
        job.setInputFormat(NullInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormat(TextOutputFormat.class);
        // help Hama to locale the jar to be distributed
        job.setJarByClass(HAMA_TEST.class);
        // give it a name
        job.setJobName("HAMA_MMAS");
        FileOutputFormat.setOutputPath(job, TMP_OUTPUT);

        BSPJobClient jobClient = new BSPJobClient(conf);
        ClusterStatus cluster = jobClient.getClusterStatus(true);
//        job.setNumBspTask(cluster.getMaxTasks());
        job.setNumBspTask(5);
        long startTime = System.currentTimeMillis();
        if (job.waitForCompletion(true)) {
        printOutput(conf);
        System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }

    }

}
