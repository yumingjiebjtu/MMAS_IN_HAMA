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
public class MMAS {
    
    private double[][] Distance;
    private double[][] DistanceBeta;
    public double[][] Pheromone;
    public double[][] Possiblity;
    
        
    public double dbRate; //最大信息素和最小信息素的比值
    
    //以下6个参数将在完成时改为通过HAMA Configuration获得
    private  double ALPHA;
    private  double BETA;
    private  double ROU; //信息素残留系数，(1-ROU)就是信息素的挥发速度
    private  double Pbest; //蚂蚁一次搜索找到最优路径的概率
    private  int AntCount;//蚂蚁的数量
    private  int Iterations;//迭代次数;
    

//    private final Matrix matrix = new Matrix();
    
    public Ant IterationBestAnt ;
    public Ant GlobalBestAnt ;
    public Ant[] AntArray ;
    
    
    private int CityNum;
    
    public MMAS(int city,float a,float b,float r,float p, int ant,int i,float rate
    ,double[][] d,double[][] dB,double[][] Ph,double [][] Po)
    {
        this.CityNum = city;
        this.ALPHA = a;
        this.BETA = b;
        this.ROU = r;
        this.Pbest = p;
        this.AntCount = ant;
        this.Iterations = i;
        this.dbRate = rate;
        this.Distance = d;
        this.DistanceBeta = dB;
        this.Pheromone = Ph;
        this.Possiblity = Po;
        
    }
            
    
    public void Initalize ( ){

//        Distance = new double [CityNum][CityNum];
//        DistanceBeta = new double [CityNum][CityNum];
//        Pheromone = new double [CityNum][CityNum];
//        Possiblity = new double[CityNum][CityNum];

                
        IterationBestAnt = new Ant(CityNum, Distance, Possiblity);        
        GlobalBestAnt = new Ant(CityNum, Distance, Possiblity);
        IterationBestAnt.TourLength = Double.MAX_VALUE;
        GlobalBestAnt.TourLength = Double.MAX_VALUE;
        
        //初始化蚂蚁群
        InitalizeAnts();
    }
    public void InitalizeAnts(){
        //初始化蚂蚁群
        AntArray = new Ant[AntCount];
        for(int i=0;i<AntCount;i++){
            AntArray[i] = new Ant(CityNum, Distance, Possiblity);
            AntArray[i].Init();
        }                                        
    }
    
    
    
    
    
    public void UpdateTrial(int nFlag){

        Ant TempAnt;
        if (nFlag == 1) //使用全局最优解
	{
		TempAnt=GlobalBestAnt;
	}
	else //使用迭代最优解
	{
		TempAnt=IterationBestAnt;
	}
        
        
        
	//临时保存信息素
	double [][] dbTempAry = new double[CityNum][CityNum];

	//计算新增加的信息素,保存到临时数组里
	int m=0;
	int n=0;
	//计算目前最优蚂蚁留下的信息素
	for (int j=1;j<CityNum;j++)
	{
		m=TempAnt.AntPath[j];
		n=TempAnt.AntPath[j-1];
		dbTempAry[n][m]=dbTempAry[n][m]+1.0/TempAnt.TourLength;
		dbTempAry[m][n]=dbTempAry[n][m];
	}

	//最后城市和开始城市之间的信息素
	n=TempAnt.AntPath[0];
	dbTempAry[n][m]=dbTempAry[n][m]+1.0/TempAnt.TourLength;
	dbTempAry[m][n]=dbTempAry[n][m];


	//==================================================================
	//更新环境信息素
	for (int i=0;i<CityNum;i++)
	{
		for (int j=0;j<CityNum;j++)
		{
			Pheromone[i][j]=Pheromone[i][j]*ROU+dbTempAry[i][j];  //最新的环境信息素 = 留存的信息素 + 新留下的信息素
		}
	}

	//==================================================================
	//检查环境信息素，如果在最小和最大值的外面，则将其重新调整
	double Imax=1.0/(TempAnt.TourLength*(1.0-ROU));
	double Imin=Imax*dbRate;

	for (int i=0;i<CityNum;i++)
	{
		for (int j=0;j<CityNum;j++)
		{
			if (Pheromone[i][j] < Imin)
			{
				Pheromone[i][j] = Imin;
			}

			if (Pheromone[i][j] > Imax)
			{
				Pheromone[i][j] = Imax;
			}

		}
	}


	for (int i=0;i<CityNum;i++)
	{
		for (int j=0;j<CityNum;j++)
		{
			Possiblity[i][j]=Math.pow(Pheromone[i][j],ALPHA)*DistanceBeta[i][j];
		}
	}
        
       
        

    }

    public void Solve(){
        

	for (int i=0;i<Iterations;i++)
	{
            //每只蚂蚁搜索一遍
            for (int j=0;j<AntCount;j++)
            {
                    AntArray[j].Search(); 
            }

            //保存最佳结果
            for (int k=0;k<AntCount;k++)
            {
                    if (AntArray[k].TourLength < GlobalBestAnt.TourLength)
                    {
                            GlobalBestAnt = AntArray[k];
                    }
                    if (AntArray[k].TourLength < IterationBestAnt.TourLength)
                    {
                            IterationBestAnt = AntArray[k];
                    }
            }

            // 更新环境信息素，使用全局最优和迭代最优交替更新的策略
            // 每过5次迭代使用一次全局最优蚂蚁更新信息素
            // 这样可以扩大搜索范围
            if ((i+1) % 5 == 0)
            {
                    UpdateTrial(1);
            }
            else
            {
                    UpdateTrial(0);
            }
            
            
            //重新初始化蚂蚁
            InitalizeAnts();
        
        }
//        while(true)
//	{
//            
//            if(GlobalBestAnt.TourLength <= 433.0){
//                System.out.println(It);
//                break;
//            }
//                
//            //每只蚂蚁搜索一遍
//            for (int j=0;j<AntCount;j++)
//            {
//                    AntArray[j].Search(); 
//            }
//
//            //保存最佳结果
//            for (int k=0;k<AntCount;k++)
//            {
//                    if (AntArray[k].TourLength < GlobalBestAnt.TourLength)
//                    {
//                            GlobalBestAnt = AntArray[k];
//                    }
//                    if (AntArray[k].TourLength < IterationBestAnt.TourLength)
//                    {
//                            IterationBestAnt = AntArray[k];
//                    }
//            }
//
//            // 更新环境信息素，使用全局最优和迭代最优交替更新的策略
//            // 每过5次迭代使用一次全局最优蚂蚁更新信息素
//            // 这样可以扩大搜索范围
//            if ((It+1) % 5 == 0)
//            {
//                    UpdateTrial(1);
//            }
//            else
//            {
//                    UpdateTrial(0);
//            }
//            
//            
//            //重新初始化蚂蚁
//            InitalizeAnts();
//            
//            It++;
//        }
    }        
}
