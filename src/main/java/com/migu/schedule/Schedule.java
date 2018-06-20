package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.Consume;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    //存储的任务
    private TreeMap<Integer, Consume> taskIds = new TreeMap<Integer, Consume>();

    //挂起的任务
    private TreeMap<Integer, TreeSet<Consume>> hangUpQueue = new TreeMap<Integer, TreeSet<Consume>>();

    //服务器节点
    private TreeMap<Integer, TreeSet<Consume>> nodeServers = new TreeMap<Integer, TreeSet<Consume>>();

    public int init() {
        taskIds = new TreeMap<Integer, Consume>();
        hangUpQueue = new TreeMap<Integer, TreeSet<Consume>>();
        nodeServers = new TreeMap<Integer, TreeSet<Consume>>();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }

        if (nodeServers.containsKey(nodeId)) {
            return ReturnCodeKeys.E005;
        }

        TreeSet<Consume> onsume = new TreeSet<Consume>();
        nodeServers.put(nodeId, onsume);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }

        if (!nodeServers.containsKey(nodeId)) {
            return ReturnCodeKeys.E007;
        }

        TreeSet<Consume> consume = nodeServers.remove(nodeId);
        Iterator<Consume> itr = consume.iterator();
        while (itr.hasNext()) {
            Consume consumptionInfo = itr.next();
            consumptionInfo.setNodeId(-1);//离开节点变为-1
            toHangUp(consumptionInfo);
        }
        return ReturnCodeKeys.E006;
    }

    private void toHangUp(Consume consume) {

        TreeSet<Consume> consumptionInfos = hangUpQueue.get(consume.getConsumption());
        if (null == consumptionInfos) {
            consumptionInfos = new TreeSet<Consume>();
            hangUpQueue.put(consume.getConsumption(), consumptionInfos);
        }
        consumptionInfos.add(consume);
    }


    public int addTask(int taskId, int consumption) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        if (taskIds.containsKey(taskId)) {
            return ReturnCodeKeys.E010;
        }
        Consume consume = new Consume(taskId, consumption);
        taskIds.put(taskId, consume);

        toHangUp(consume);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if (taskId <= 0) {
            return ReturnCodeKeys.E009;
        }

        if (!taskIds.containsKey(taskId)) {
            return ReturnCodeKeys.E012;
        }

        Consume consumptionInfo = taskIds.remove(taskId);

        deleteTask(consumptionInfo, this.hangUpQueue);

        deleteTask(consumptionInfo, this.nodeServers);
        return ReturnCodeKeys.E011;
    }

    private void deleteTask(Consume consumptionInfo, TreeMap<Integer, TreeSet<Consume>> consumptionInfosMap) {
        Iterator<Map.Entry<Integer, TreeSet<Consume>>> itr = consumptionInfosMap.entrySet().iterator();
        while (itr.hasNext()) {
            itr.next().getValue().remove(consumptionInfo);
        }
    }


    public int scheduleTask(int threshold) {
        if (threshold <= 0) {
            return ReturnCodeKeys.E002;
        }

        for (Consume consumptionInfo : taskIds.values()) {
            toHangUp(consumptionInfo);
        }
        int nodeSize = nodeServers.size();
        int[] consumptions = new int[nodeSize];
        TreeSet<Consume>[] cpis = new TreeSet[nodeSize];
        for (int i = 0; i< nodeSize; i++) {
            cpis[i] = new TreeSet<Consume>();
        }

        TreeMap<Integer, TreeSet<Consume>> nodeServersBak = new TreeMap<Integer, TreeSet<Consume>>();
        int idx = 0;
        Collection<TreeSet<Consume>> cpiSetCol = hangUpQueue.values();
        for (TreeSet<Consume> cpiSet : cpiSetCol) {
            for (Consume cpi : cpiSet) {
                consumptions[idx] += cpi.getConsumption();
                cpis[idx].add(cpi);
                if (idx == nodeSize) {
                    idx = 0;
                }
            }
        }
        Arrays.sort(consumptions);

        if (consumptions[nodeSize -1] - consumptions[0] > threshold) {
            return ReturnCodeKeys.E014;
        }

        Iterator<Map.Entry<Integer, TreeSet<Consume>>> itr = nodeServers.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Integer, TreeSet<Consume>> en = itr.next();
            Integer nodeId = en.getKey();
            TreeSet<Consume> consumptionInfosSet =  en.getValue();

        }

        return ReturnCodeKeys.E013;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        if (null == tasks || tasks.size() == 0) {
            return ReturnCodeKeys.E016;
        }
        tasks.clear();
        for (Consume consumptionInfo : taskIds.values()) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setNodeId(consumptionInfo.getNodeId());
            taskInfo.setTaskId(consumptionInfo.getTaskId());
            tasks.add(taskInfo);
        }
        return ReturnCodeKeys.E015;
    }
}
