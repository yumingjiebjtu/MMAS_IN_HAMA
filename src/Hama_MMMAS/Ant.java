/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hama_MMMAS;

import java.util.Random;

/**
 *
 * @author mjh
 */
public class Ant implements Cloneable{
    
    
    public int[] AntPath; //蚂蚁行走路径表
    public int[] AllowedCities; //允许搜索的城市
    public double[][] Distance; //距离矩阵
    public double[][] Possibility;  //转移概率矩阵
    
    public double TourLength; //路径长度
    private int CityNum; //城市数量
    
    public int CurrentCity; //当前城市
    
    public int MovedCityCount;//已经去过的城市数量
    
    public boolean GreedSearchEnable = false;//贪心搜索开启标示,默认为false
    
    private Random random;

    
    public Ant(int num,double[][] Distance,double [][] Possibility){
        this.random = new Random(System.currentTimeMillis());
      
        CityNum = num;
        AntPath = new int [CityNum];
        AllowedCities = new int [CityNum];

        this.Distance = Distance;
        this.Possibility = Possibility;    
    }
    
    
    public void Init(){
        

        for(int i =0;i<CityNum;i++)
        {
            AllowedCities[i]=1;//设置全部城市为没有去过
            AntPath[i]=0;//蚂蚁走的路径全部设置为0
        }

        //蚂蚁走过的路径长度设置为0
        TourLength = 0;
        
        //随机选择一个城市作为出发城市
        CurrentCity = random.nextInt(CityNum);
        //将选择的出发城市
        AntPath[0] = CurrentCity;
        //标识出发城市为已经去过了
        AllowedCities[CurrentCity] = 0;
        //已经去过的城市数量设置为1
        MovedCityCount=1;

//        //不使用贪心原则选择下一城市
//        GreedSearchEnable=false;        
        
        
        
    }
    
    
    
    
    
    
    
//    /**初始化函数
//     * @param num 城市数量
//     * @param Distance 距离矩阵
//     * @param Possibility 转移概率矩阵
//    */
//    public void Initalize(int num,double[][] Distance,double [][] Possibility){
//      
//      this.random = new Random(System.currentTimeMillis());
//      
//      CityNum = num;
//      AntPath = new int [CityNum];
//      AllowedCities = new int [CityNum];
//
//      this.Distance = Distance;
//      this.Possibility = Possibility;
//      
//      //不使用贪心原则选择下一城市
//      GreedSearchEnable=false;
//    }
    

    public void Search(){
        
        //Step 1
        Init();
        
        //Step 2
        while(MovedCityCount < CityNum)
        {
            Move();
        }
        
        //Step 3
        
        CalAntPathLenth();
    }
    public int GreedChooseNextCity(double DB_MAX){
        //DB_MAX should be double max positive value
        int SelectedCity = -1;
        double dbLenth = DB_MAX;
        for(int i =0;i<CityNum;i++)
        {
            if(AllowedCities[i] == 1) //城市没去过
            {
                if(Distance[CurrentCity][i]<dbLenth)
                {
                    dbLenth = Distance[CurrentCity][i];
                    SelectedCity = i;
                }
            }
        }
        return SelectedCity;
   
    }
    public int ChooseNextCity(){
        int SelectedCity = -1;
        //计算当前城市和没去过的城市之间的信息素总和
        double Total = 0.0;
        double[] Probably = new double[CityNum];//保存城市被选中的概率
        for(int i =0;i<CityNum;i++)
        {
            if(AllowedCities[i] == 1) //城市没去过
            {
                Probably[i] = Possibility[CurrentCity][i];
                Total += Probably[i];                  
            }
            else
            {
                Probably[i] = 0.0;
            }
        }
        //轮盘赌
        @SuppressWarnings("UnusedAssignment")
        double Temp = 0.0;
        if(Total > 0.0)
        {
            Temp = random.nextDouble()*Total;//取0到Total间一个随机数
            for(int i =0;i<CityNum;i++)
            {
                if(AllowedCities[i] == 1) //城市没去过
                {
                    //转动轮盘
                    Temp = Temp - Probably[i];
                    //轮盘停止转动，记下城市编号
                    if(Temp < 0.0)
                    {
                        SelectedCity=i;
                        break;
                    }
                }
            }
        }
        
	//如果城市间的信息素非常小 ( 小到比double能够表示的最小的数字还要小 )
	//那么由于浮点运算的误差原因，上面计算的概率总和可能为0
	//会出现经过上述操作，没有城市被选择出来
	//出现这种情况，就把第一个没去过的城市作为返回结果
	if (SelectedCity == -1)
	{
		for (int i=0;i<CityNum;i++)
		{
			if (AllowedCities[i] == 1) //城市没去过
			{
				SelectedCity=i;
				break;
			}
		}
	}

	//==============================================================================
	//返回结果        
        

        return SelectedCity;
    }
    
    public void Move(){
        int CityNo = 0;
        //选择下一个城市
        if(GreedSearchEnable == true)
        {
            CityNo = GreedChooseNextCity(Double.MAX_VALUE);
        }
        else
        {
            CityNo = ChooseNextCity();
        }
        AntPath[MovedCityCount] = CityNo;//蚂蚁走的路径
	AllowedCities[CityNo]=0;//没去过的城市
	CurrentCity=CityNo; //当前所在城市编号
	MovedCityCount++; //已经去过的城市数量
    }
    
    public void CalAntPathLenth(){
        TourLength = 0.0;
        int m = 0;
        int n = 0;
        for(int i =1;i<CityNum;i++)
        {
            m = AntPath[i];
            n = AntPath[i-1];
            TourLength = TourLength + Distance[m][n];
        }
        //加上从最后城市返回出发城市的距离
        n = AntPath[0];
        TourLength = TourLength = TourLength + Distance[m][n];
    }    
}
