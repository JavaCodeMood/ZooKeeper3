package com.zookeeper2;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.CreateMode;


public class WorkServer {
	//客户端状态  记录服务器的状态
    private volatile boolean running = false;
    //Zookeeper的客户端
    private ZkClient zkClient;

    //zk主节点路径
    public static final String MASTER_PATH = "/master";

    //监听(用于监听主节点删除事件)
    private IZkDataListener dataListener;

    //服务器基本信息  记录集群中服务的基本信息
    private RunningData serverData;
    //主节点基本信息  记录集群中master节点的基本信息
    private RunningData masterData;

    //创建调度器
    private ScheduledExecutorService delayExector = Executors.newScheduledThreadPool(1);
    //定义延迟时间5s
    private int delayTime = 5;



    public WorkServer(RunningData runningData){
        this.serverData = runningData;
        //实例化
        this.dataListener = new IZkDataListener() {
            public void handleDataChange(String s, Object o) throws Exception {

            }

            public void handleDataDeleted(String s) throws Exception {
                //1.不启动网络抖动的优化策略
                takeMaster();
            	
            	 /*2.启用网络抖动的优化策略
                                      如果masterData的名字和serverData的名字是相同的，
                                      说明当前服务器就是上一轮对应的服务器
                 */
                /*if(masterData != null && masterData.getName().equals(serverData.getName())){//若之前master为本机,则立即抢主,否则延迟5秒抢主(防止小故障引起的抢主可能导致的网络数据风暴)
                    takeMaster();
                }else{
                	//延迟5秒再去增强master权利
                    delayExector.schedule(new Runnable() {
                        public void run() {
                            takeMaster();
                        }
                    },delayTime, TimeUnit.SECONDS);
                }*/

            }
        };
    }

    //启动  服务器启动函数
    public void start() throws Exception{
    	//判断服务器是否处于运行状态，如果已经运行，需要抛出异常
        if(running){
            throw new Exception("server has startup....");
        }
        running = true;
        //订阅master节点的删除事件
        zkClient.subscribeDataChanges(MASTER_PATH,dataListener);
        //增强master权利
        takeMaster();
    }

    //停止  停止服务器
    public void stop() throws Exception{
    	//判断当前服务器是否已经处于停止状态，是的话，抛出异常
        if(!running){
            throw new Exception("server has stopped.....");
        }
        running = false;
        
        delayExector.shutdown();
        //取消master事件订阅
        zkClient.unsubscribeDataChanges(MASTER_PATH,dataListener);
        //释放master权利
        releaseMaster();
    }

    //抢注主节点  增强master权利
    private void takeMaster(){
    	//判断服务器的状态
        if(!running) return ;

        try {
        	//创建master临时节点
            zkClient.create(MASTER_PATH, serverData, CreateMode.EPHEMERAL);
            //创建成功就将serverData赋值给masterData
            masterData = serverData;
            System.out.println(serverData.getName()+" is master");
            //测试，我们让服务器每隔5秒释放一次master权利
            delayExector.schedule(new Runnable() {//测试抢主用,每5s释放一次主节点
                public void run() {
                    if(checkMaster()){
                        releaseMaster();
                    }
                }
            },5,TimeUnit.SECONDS);


        }catch (ZkNodeExistsException e){//节点已存在
        	//我们读取当前master节点信息放入masterData中，
            RunningData runningData = zkClient.readData(MASTER_PATH,true);
            //如果没有读取到master节点信息，说明我们在读取master的瞬间，master节点已经宕机，此时需要再次进行master选举
            if(runningData == null){//读取主节点时,主节点被释放
                takeMaster();
            }else{
                masterData = runningData;
            }
        } catch (Exception e) {
            // ignore;
        }

    }
    //释放主节点  释放master权利
    private void releaseMaster(){
    	//释放master权利，我们首先需要判断一下当前我们自己是不是master，如果是master，我们就删除自己创建的临时节点
        if(checkMaster()){
            zkClient.delete(MASTER_PATH);
        }
    }
    
    /*检验自己是否是主节点 监测自己是不是master，我们首先需要导zookeeper中读取master节点的基本信息
         然后跟自己的基本信息进行比对，如果一样，说明自己是master
    */
    private boolean checkMaster(){
        try {
        	//执行读取
            RunningData runningData = zkClient.readData(MASTER_PATH);
            //赋值
            masterData = runningData;
            //执行比对 如果masterData的名称与serverData的名称一致，我们就认为自己是master
            if (masterData.getName().equals(serverData.getName())) {
                return true;
            }
            return false;

        }catch (ZkNoNodeException e){//节点不存在
            return  false;
        }catch (ZkInterruptedException e){//网络中断
            return checkMaster();  //处理中断的方式是重试
        }catch (Exception e){//捕获其他异常
            return false;
        }
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

}
