
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;

public class ProducerWorkServiceImpl extends BaseServiceImpl implements ProducerWorkService{
	ProducerWorkDAO producerWorkDAO = (ProducerWorkDAO)DaoFactory.getDao(ProducerWorkDAO.class);
	ScheduleRuleDAO scheduleRuleDAO = (ScheduleRuleDAO)DaoFactory.getDao(ScheduleRuleDAO.class);
	SubscribeDateDAO subscribeDateDAO = (SubscribeDateDAO)DaoFactory.getDao(SubscribeDateDAO.class);
	SubscribeTimeDAO subscribeTimeDAO = (SubscribeTimeDAO)DaoFactory.getDao(SubscribeTimeDAO.class);
	SubscribeInfoDAO subscribeInfoDAO = (SubscribeInfoDAO)DaoFactory.getDao(SubscribeInfoDAO.class);
	
	public Object createProducerWork(ProducerWork producerWork) {		
		return producerWorkDAO.createProducerWork(producerWork);
	}

	public void updateProducerWork(ProducerWork producerWork) {
		producerWorkDAO.updateProducerWork(producerWork);
	}

	public void deleteProducerWork(ProducerWork producerWork) {
		producerWorkDAO.deleteProducerWork(producerWork);
	}

	public Collection getProducerWorkList(ProducerWork producerWork) {
		return producerWorkDAO.getProducerWorkList(producerWork);
	}

	public void employeeScheduleScreenQuery(Pagin pg, ProducerWorkDTO pwDto) {
		producerWorkDAO.employeeScheduleScreenQuery(pg, pwDto);
		Collection<ProducerWorkDTO> coll = pg.getRows();
		this.setProducerWorkDtoRuleValue(coll, pwDto);		
	}
	public void employeeTeamScheduleQuery(Pagin pg, ProducerWorkDTO pwDto) {
		producerWorkDAO.employeeTeamScheduleQuery(pg, pwDto);
		Collection<ProducerWorkDTO> coll = pg.getRows();
		this.setProducerWorkDtoRuleValue(coll, pwDto);		
	}
	public void feeScheduleScreenQuery(Pagin pg, ProducerWorkDTO pwDto){
		producerWorkDAO.feeScheduleScreenQuery(pg, pwDto);
		Collection<ProducerWorkDTO> coll = pg.getRows();
		this.setProducerWorkDtoRuleValue(coll, pwDto);
	}	
	public void scheduleScreenQuery(Pagin pg, ProducerWorkDTO pwDto){
		producerWorkDAO.scheduleScreenQuery(pg, pwDto);
		Collection<ProducerWorkDTO> coll = pg.getRows();
		this.setProducerWorkDtoRuleValue(coll, pwDto);
	}
	public void subscribeScreenQuery(Pagin pg, ProducerWorkDTO pwDto) {
		producerWorkDAO.subscribeScreenQuery(pg, pwDto);
		Collection<ProducerWorkDTO> coll = pg.getRows();
		this.setProducerWorkDtoRuleValue(coll, pwDto);		
	}
	public void employeeSubscribeScreenQuery(Pagin pg, ProducerWorkDTO pwDto){
		producerWorkDAO.employeeSubscribeScreenQuery(pg, pwDto);
		Collection<ProducerWorkDTO> coll = pg.getRows();
		this.setProducerWorkDtoRuleValue(coll, pwDto);
	}
	
	/* saveWeekSchedule，old为数据库查询结果 input为页面输入的
	 * old     1 2* 3   4*  5   6*     * 表示有预约信息
	 * input        3=  4=  5X  6X  7   = 表示班次相同， X表示班次不相同
	 * mayDel  1  3  5
	 * notDel  2  4  6
	 * mayAdd  3  5  7
	 * notAdd  4  6   不用记录这些数据
	 * okDel   1  5   删除记录
	 * same    3  判断SD是否相等
	 * okAdd   5  7   增加记录  notInsert
	 */
	public int saveWeekSchedule(ProducerWork producerWork, ProducerWorkDTO producerWorkDto) {
		int result = 0;

		Collection<ProducerWork> old = new ArrayList<ProducerWork>();
		Map<String, List<ProducerWork>> mapOldProducerWork = new HashMap<String, List<ProducerWork>>();
		this.batchSelectWeekSchedule(old, mapOldProducerWork, producerWorkDto);
		Collection<SubscribeDate> oldSubscribeDate = new ArrayList<SubscribeDate>();
		this.batchSelectWeekSubscribeDate(oldSubscribeDate, producerWorkDto);
		Map<Long, List<SubscribeDate>> mapOldSubscribeDate = new HashMap<Long, List<SubscribeDate>>();
		this.createMapSubscribeDate(mapOldSubscribeDate, oldSubscribeDate);
		
		Collection<ProducerWork> mayDel = new ArrayList<ProducerWork>();
		Collection<ProducerWork> notDel = new ArrayList<ProducerWork>();
		//this.decomposeOldProducerWorkBySubscribeDate(old, mapOldSd, mayDel, notDel);
		this.decomposeOldProducerWork(old, mayDel, notDel);
		
		Map<String, List<ScheduleRule>> mapScheduleRule = new HashMap<String, List<ScheduleRule>>();
		this.getMapScheduleRule(mapScheduleRule, producerWork.getOrgId());		
		Collection<ProducerWork> input = new ArrayList<ProducerWork>();
		Map<String, List<ProducerWork>> mapInputProducerWork = new HashMap<String, List<ProducerWork>>();
		Map<Long, List<SubscribeDate>> mapInputSubscribeDate = new HashMap<Long, List<SubscribeDate>>();
		this.decomposeWeekSchedule(input, mapInputProducerWork, mapInputSubscribeDate, producerWork, producerWorkDto, mapScheduleRule);
		
		Collection<ProducerWork> mayAdd = new ArrayList<ProducerWork>();
		Collection<ProducerWork> notAdd = new ArrayList<ProducerWork>();
		this.decomposeInputByNotDel(input, notDel, mayAdd, notAdd);
		
		Collection<ProducerWork> okDel = new ArrayList<ProducerWork>();
		Collection<ProducerWork> same = new ArrayList<ProducerWork>();
		this.decomposeMayDelbyMayAdd(mayDel, mayAdd, okDel, same);
		this.batchDeleteProducerWorkSubscribeDate(okDel, mapOldProducerWork, producerWork, mapOldSubscribeDate);
		this.batchUpdateSubscribeDate(same, mapOldProducerWork, mapInputProducerWork, producerWork, mapOldSubscribeDate, mapInputSubscribeDate);
		
		Collection<ProducerWork> outColl = new ArrayList<ProducerWork>();
		this.decomposeNotDelbyNotAdd(notDel, notAdd, outColl, mapOldProducerWork, mapOldSubscribeDate, mapInputProducerWork, mapInputSubscribeDate);
		if(outColl.size() > 0){
			result += 200;
		}
		Collection<ProducerWork> okAdd = new ArrayList<ProducerWork>();
		this.decomposeMayAddBySame(mayAdd, same, okAdd);
		
		Collection<ProducerWork> nowProducerWork = new ArrayList<ProducerWork>();
		this.batchSelectProducerWorkTime(nowProducerWork, producerWorkDto);
	
		Collection<ProducerWork> notInsert = new ArrayList<ProducerWork>();
		this.batchInsertProducerWorkSubscribeDate(okAdd, mapInputProducerWork, nowProducerWork, mapInputSubscribeDate, notInsert);
		if(notInsert.size() > 0){
			result += 10000;
		}
		outColl.addAll(notInsert);
		
		return result;
	}
	
	public int batchSaveSchedule(ProducerWork producerWork, Collection<ProducerWorkDTO> collDtoInput, Collection<ProducerWorkDTO> collDtoOut) {
		int result = 0;
		if( collDtoInput != null && collDtoInput.size() > 0){
			log.info("collDtoInput.size()=========>" + collDtoInput.size());
			Map<String, List<ScheduleRule>> mapSr = new HashMap<String, List<ScheduleRule>>();
			this.getMapScheduleRule(mapSr, producerWork.getOrgId());
			
			Collection<ProducerWork> old = new ArrayList<ProducerWork>();
			Map<String, List<ProducerWork>> mapOldProducerWork = new HashMap<String, List<ProducerWork>>();
			Collection<SubscribeDate> oldSd = new ArrayList<SubscribeDate>();
			Collection<ProducerWork> input = new ArrayList<ProducerWork>();
			Map<String, List<ProducerWork>> mapInputProducerWork = new HashMap<String, List<ProducerWork>>();
			Map<Long, List<SubscribeDate>> mapInputSd = new HashMap<Long, List<SubscribeDate>>();		
			for (ProducerWorkDTO dto : collDtoInput) {
				this.batchSelectWeekSchedule(old, mapOldProducerWork, dto);
				this.batchSelectWeekSubscribeDate(oldSd, dto);
				this.decomposeWeekSchedule(input, mapInputProducerWork, mapInputSd, producerWork, dto, mapSr);
			}
			Map<Long, List<SubscribeDate>> mapOldSd = new HashMap<Long, List<SubscribeDate>>();
			this.createMapSubscribeDate(mapOldSd, oldSd);
			
			Collection<ProducerWork> mayDel = new ArrayList<ProducerWork>();
			Collection<ProducerWork> notDel = new ArrayList<ProducerWork>();
			//this.decomposeOldProducerWorkBySubscribeDate(old, mapOldSd, mayDel, notDel);
			this.decomposeOldProducerWork(old, mayDel, notDel);
			
			Collection<ProducerWork> mayAdd = new ArrayList<ProducerWork>();
			Collection<ProducerWork> notAdd = new ArrayList<ProducerWork>();
			this.decomposeInputByNotDel(input, notDel, mayAdd, notAdd);
			
			Collection<ProducerWork> okDel = new ArrayList<ProducerWork>();
			Collection<ProducerWork> same = new ArrayList<ProducerWork>();
			this.decomposeMayDelbyMayAdd(mayDel, mayAdd, okDel, same);
			this.batchDeleteProducerWorkSubscribeDate(okDel, mapOldProducerWork, producerWork, mapOldSd);
			this.batchUpdateSubscribeDate(same, mapOldProducerWork, mapInputProducerWork, producerWork, mapOldSd, mapInputSd);

			Collection<ProducerWork> outColl = new ArrayList<ProducerWork>();
			this.decomposeNotDelbyNotAdd(notDel, notAdd, outColl, mapOldProducerWork, mapOldSd, mapInputProducerWork, mapInputSd);
			if(outColl.size() > 0){
				result += 100;
			}
			
			Collection<ProducerWork> okAdd = new ArrayList<ProducerWork>();
			this.decomposeMayAddBySame(mayAdd, same, okAdd);
			
			Map<Long, List<ProducerWorkDTO>> mapPwDto = new HashMap<Long, List<ProducerWorkDTO>>();
			Collection<ProducerWork> nowProducerWork = new ArrayList<ProducerWork>();
			for (ProducerWorkDTO dto : collDtoInput) {
				this.batchSelectProducerWorkTime(mapPwDto, nowProducerWork, dto);
			}
			Map<Long, List<ProducerWork>> mapNowProducerWork = new HashMap<Long, List<ProducerWork>>();
			this.createMapProducerWorkTime(mapNowProducerWork, nowProducerWork);
			
			Collection<ProducerWork> notInsert = new ArrayList<ProducerWork>();
			this.batchInsertProducerWorkSubscribeDate(okAdd, mapInputProducerWork, mapNowProducerWork, mapInputSd, notInsert);
			if(notInsert.size() > 0){
				result += 10000;
			}
			outColl.addAll(notInsert);
		}		
		return result;
	}
	
	/* saveWeekSchedule，old为数据库查询结果 input为页面输入的
	 * old     1 2* 3   4*  5   6*     * 表示有预约信息
	 * input        3=  4=  5X  6X  7   = 表示班次相同， X表示班次不相同
	 * mayDel  1  3  5
	 * notDel  2  4  6
	 * mayAdd  3  5  7
	 * notAdd  4  6   不用记录这些数据
	 * okDel   1  5   删除记录
	 * same    3  判断SD是否相等
	 * okAdd   5  7   增加记录  notInsert
	 */
	
	public int batchSaveScheduleSubscribe(ProducerWork producerWork, Collection<ProducerWorkDTO> collDtoInput, Collection<ProducerWorkDTO> collDtoOut) {
		int result = 0;
		if( collDtoInput != null && collDtoInput.size() > 0){
			log.info("collDtoInput.size()=========>" + collDtoInput.size());
			Map<String, List<ScheduleRule>> mapSr = new HashMap<String, List<ScheduleRule>>();
			this.getMapScheduleRule(mapSr, producerWork.getOrgId());
			
			Collection<ProducerWork> old = new ArrayList<ProducerWork>();
			Map<String, List<ProducerWork>> mapOldProducerWork = new HashMap<String, List<ProducerWork>>();
			Collection<SubscribeDate> oldSd = new ArrayList<SubscribeDate>();
			Collection<ProducerWork> input = new ArrayList<ProducerWork>();
			Map<String, List<ProducerWork>> mapInputProducerWork = new HashMap<String, List<ProducerWork>>();
			Map<Long, List<SubscribeDate>> mapInputSd = new HashMap<Long, List<SubscribeDate>>();		
			for (ProducerWorkDTO dto : collDtoInput) {
				this.batchSelectWeekSchedule(old, mapOldProducerWork, dto);
				this.batchSelectWeekSubscribeDate(oldSd, dto);
				this.decomposeWeekSchedule(input, mapInputProducerWork, mapInputSd, producerWork, dto, mapSr);
			}
			Map<Long, List<SubscribeDate>> mapOldSd = new HashMap<Long, List<SubscribeDate>>();
			this.createMapSubscribeDate(mapOldSd, oldSd);
			
			Collection<ProducerWork> mayDel = new ArrayList<ProducerWork>();
			Collection<ProducerWork> notDel = new ArrayList<ProducerWork>();
			//this.decomposeOldProducerWorkBySubscribeDate(old, mapOldSd, mayDel, notDel);
			this.decomposeOldProducerWork(old, mayDel, notDel);
			
			Collection<ProducerWork> mayAdd = new ArrayList<ProducerWork>();
			Collection<ProducerWork> notAdd = new ArrayList<ProducerWork>();
			this.decomposeInputByNotDel(input, notDel, mayAdd, notAdd);
			
			Collection<ProducerWork> okDel = new ArrayList<ProducerWork>();
			Collection<ProducerWork> same = new ArrayList<ProducerWork>();
			this.decomposeMayDelbyMayAdd(mayDel, mayAdd, okDel, same);
			this.batchDeleteProducerWorkSubscribeDate(okDel, mapOldProducerWork, producerWork, mapOldSd);
			this.batchUpdateSubscribeDateTime(same, mapOldProducerWork, mapInputProducerWork, producerWork, mapOldSd, mapInputSd);
			Collection<ProducerWork> outColl = new ArrayList<ProducerWork>();
			this.decomposeNotDelbyNotAdd(notDel, notAdd, outColl, mapOldProducerWork, mapOldSd, mapInputProducerWork, mapInputSd);
			if(outColl.size() > 0){
				result += 100;
			}
			
			Collection<ProducerWork> okAdd = new ArrayList<ProducerWork>();
			this.decomposeMayAddBySame(mayAdd, same, okAdd);
			
			Map<Long, List<ProducerWorkDTO>> mapPwDto = new HashMap<Long, List<ProducerWorkDTO>>();
			Collection<ProducerWork> nowProducerWork = new ArrayList<ProducerWork>();
			for (ProducerWorkDTO dto : collDtoInput) {
				this.batchSelectProducerWorkTime(mapPwDto, nowProducerWork, dto);
			}
			Map<Long, List<ProducerWork>> mapNowProducerWork = new HashMap<Long, List<ProducerWork>>();
			this.createMapProducerWorkTime(mapNowProducerWork, nowProducerWork);
			
			Collection<ProducerWork> notInsert = new ArrayList<ProducerWork>();
			this.batchInsertProducerWorkSubscribeDateTime(okAdd, mapInputProducerWork, mapNowProducerWork, mapInputSd, notInsert);
			if(notInsert.size() > 0){
				result += 10000;
			}
			outColl.addAll(notInsert);
		}		
		return result;
	}
	
	public int updateWeekScheduleSubscribe(ProducerWork producerWork, ProducerWorkDTO pwDto) {
		Collection<ProducerWork> input = new ArrayList<ProducerWork>();
		this.decomposeWeekScheduleSubscribe(input, pwDto);
		
		Collection<ProducerWork> old = new ArrayList<ProducerWork>();
		Map<String, List<ProducerWork>> mapOldProducerWork = new HashMap<String, List<ProducerWork>>();
		this.batchSelectWeekSchedule(old, mapOldProducerWork, pwDto);
		Collection<SubscribeDate> oldSd = new ArrayList<SubscribeDate>();
		this.batchSelectWeekSubscribeDate(oldSd, pwDto);
		Map<Long, List<SubscribeDate>> mapOldSd = new HashMap<Long, List<SubscribeDate>>();
		this.createMapSubscribeDate(mapOldSd, oldSd);
		this.batchUpdateProducerWorkSubscribeDate(input, mapOldProducerWork, mapOldSd, producerWork);		
		return 0;
	}

	public int batchUpdateWeekScheduleSubscribe(ProducerWork producerWork,	Collection<ProducerWorkDTO> collDto) {
		if(collDto != null && collDto.size() > 0){
			Collection<ProducerWork> input = new ArrayList<ProducerWork>();
			Collection<ProducerWork> old = new ArrayList<ProducerWork>();
			Map<String, List<ProducerWork>> mapOldProducerWork = new HashMap<String, List<ProducerWork>>();
			Collection<SubscribeDate> oldSd = new ArrayList<SubscribeDate>();
			for (ProducerWorkDTO pwDto : collDto) {
				this.decomposeWeekScheduleSubscribe(input, pwDto);
				this.batchSelectWeekSchedule(old, mapOldProducerWork, pwDto);
				this.batchSelectWeekSubscribeDate(oldSd, pwDto);
			}
			Map<Long, List<SubscribeDate>> mapOldSd = new HashMap<Long, List<SubscribeDate>>();
			this.createMapSubscribeDate(mapOldSd, oldSd);
			this.batchUpdateProducerWorkSubscribeDate(input, mapOldProducerWork, mapOldSd, producerWork);	
		}
		return 0;
	}
	
	private void decomposeNotDelbyNotAdd(Collection<ProducerWork> notDel, Collection<ProducerWork> notAdd, Collection<ProducerWork> outColl, 
			Map<String, List<ProducerWork>> mapOldProducerWork, Map<Long, List<SubscribeDate>> mapOldSubscribeDate, 
			Map<String, List<ProducerWork>> mapInputProducerWork, Map<Long, List<SubscribeDate>> mapInputSubscribeDate){
		if(notDel != null && notDel.size() > 0){
			for (ProducerWork delProducerWork : notDel) {
				boolean flag = true;
				if(notAdd != null && notAdd.size() > 0){
					for (ProducerWork addProducerWork : notAdd) {
						if(this.judgeProducerWorkSubscribeDate(delProducerWork, addProducerWork, mapOldProducerWork,  mapOldSubscribeDate, mapInputProducerWork, mapInputSubscribeDate)){
							flag = false;
							break;
						}
					}
				}				
				if(flag){
					outColl.add(delProducerWork);
				}
			}
		}		
	}
	private boolean judgeProducerWorkSubscribeDate(ProducerWork oldProducerWork, ProducerWork inputProducerWork,
			Map<String, List<ProducerWork>> mapOldProducerWork, Map<Long, List<SubscribeDate>> mapOldSubscribeDate, 
			Map<String, List<ProducerWork>> mapInputProducerWork, Map<Long, List<SubscribeDate>> mapInputSubscribeDate){
		if(judgeProducerWorkRuleId(oldProducerWork, inputProducerWork)){
			String strKey = this.getMapKey(oldProducerWork);
			Collection<ProducerWork> oldProducerWorkList = mapOldProducerWork.get(strKey);
			Collection<ProducerWork> inputProducerWorkList = mapInputProducerWork.get(strKey);
			if(oldProducerWorkList != null && inputProducerWorkList != null){
				List<SubscribeDate> oldSubscribeDate = new ArrayList<SubscribeDate>();
				for (ProducerWork oldWork : oldProducerWorkList) {
					List<SubscribeDate> oldList = mapOldSubscribeDate.get(oldWork.getId());
					if(oldList != null){
						oldSubscribeDate.addAll(oldList);
					}
				}
				List<SubscribeDate> inputSubscribeDate = new ArrayList<SubscribeDate>();
				for (ProducerWork inputWork : inputProducerWorkList) {
					List<SubscribeDate> inputList = mapInputSubscribeDate.get(inputWork.getId());
					if(inputList != null){
						inputSubscribeDate.addAll(inputList);
					}
				}
				if(oldSubscribeDate.size() == inputSubscribeDate.size()){
					if(oldSubscribeDate.size() > 0){
						Collections.sort(oldSubscribeDate);
						Collections.sort(inputSubscribeDate);
						for (int index = 0; index < oldSubscribeDate.size(); index++) {
							SubscribeDate oldSd = oldSubscribeDate.get(index);
							SubscribeDate inputSd = inputSubscribeDate.get(index);
							if(!this.judgeSubscribeDate(inputSd, oldSd)){
								return false;
							}
						}
					}
					return true;
				}				
			}
		}
		return false;
	}
	
	private void batchUpdateProducerWorkSubscribeDate(Collection<ProducerWork> input, Map<String, List<ProducerWork>> mapOldProducerWork, Map<Long, List<SubscribeDate>> mapOldSd, ProducerWork producerWork){
		int count = 0;
		if(input != null && input.size() > 0){
			for (ProducerWork inputProducerWork : input) {
				String strKey = this.getMapKey(inputProducerWork);
				Collection<ProducerWork> collProducerWork = mapOldProducerWork.get(strKey);
				if(collProducerWork != null && collProducerWork.size() > 0){
					for (ProducerWork oldProducerWork : collProducerWork) {
						if(!oldProducerWork.getPublishStatus().equals(producerWork.getPublishStatus())){
							count += this.updateProducerWorkSubscribeDate(oldProducerWork, mapOldSd, producerWork);
						}
					}
				}
				if( count >= 200 ){
					count = 0;
					dao.flush();
				}
			}
			dao.flush();
		}		
	}
	
	private int updateProducerWorkSubscribeDate(ProducerWork oldProducerWork, Map<Long, List<SubscribeDate>> mapSd, ProducerWork producerWork){
		int count = 0;
		List<SubscribeDate> sdList = mapSd.get(oldProducerWork.getId());
		if(sdList != null && sdList.size() > 0){
			Collection<SubscribeTime> collSt = new ArrayList<SubscribeTime>();
			for (SubscribeDate subscribeDate : sdList) {
				if(subscribeDate.getPublishStatus().intValue() == 1){
					if(!this.createSubscribeDateTime(collSt, subscribeDate, producerWork)){
						return 0;
					}
				}							
			}
			Collection<SubscribeInfo> collSi = new ArrayList<SubscribeInfo>();
			for (SubscribeTime subscribeTime : collSt) {
				subscribeTimeDAO.createSubscribeTime(subscribeTime);
				count++;
				count += this.createSubscribeInfo(collSi, subscribeTime);
			}
			/*for (SubscribeInfo subscribeInfo : collSi) {
				subscribeInfoDAO.createSubscribeInfo(subscribeInfo);
				count++;
			}*/
			for (SubscribeDate subscribeDate : sdList) {
				subscribeDate.setPublishStatus(producerWork.getPublishStatus());
				subscribeDate.setUpdateTime(producerWork.getUpdateTime());
				subscribeDate.setOperatorId(producerWork.getOperatorId());
				subscribeDate.setOpOrgId(producerWork.getOpOrgId());
				subscribeDateDAO.updateSubscribeDate(subscribeDate);
				count++;
			}
		}
		oldProducerWork.setPublishStatus(producerWork.getPublishStatus());
		oldProducerWork.setUpdateTime(producerWork.getUpdateTime());
		oldProducerWork.setOperatorId(producerWork.getOperatorId());
		oldProducerWork.setOpOrgId(producerWork.getOpOrgId());
		producerWorkDAO.updateProducerWork(oldProducerWork);
		count++;		
		return count;
	}
	private int createSubscribeInfo(Collection<SubscribeInfo> collSi, SubscribeTime subscribeTime){
		int count = 0;
		for(int num=1; num <= subscribeTime.getSubscribeMax().intValue(); num++){
			SubscribeInfo subscribeInfo = new SubscribeInfo();			
			subscribeInfo.setSubscribeTimeId(subscribeTime.getId());
			subscribeInfo.setProducerType(subscribeTime.getProducerType());
			subscribeInfo.setProducerId(subscribeTime.getProducerId());
			subscribeInfo.setTeamId(subscribeTime.getTeamId());
			subscribeInfo.setDepartmentId(subscribeTime.getDepartmentId());			
			subscribeInfo.setWorkDate(subscribeTime.getWorkDate());
			subscribeInfo.setStartTime(subscribeTime.getStartTime());
			subscribeInfo.setEndTime(subscribeTime.getEndTime());			
			subscribeInfo.setSubscribeStartTime(subscribeTime.getSubscribeStartTime());
			subscribeInfo.setSubscribeEndTime(subscribeTime.getSubscribeEndTime());
			subscribeInfo.setFeeId(subscribeTime.getFeeId());
			subscribeInfo.setSubscribeStatus(Integer.valueOf(ScheduleConstants.SUBSCRIBE_STATUS_ABLE));
			subscribeInfo.setSubscribeNo(WorkTableUtil.createSubscribeNo(subscribeTime, num));//edit by ljh
			subscribeInfo.setOrgId(subscribeTime.getOrgId());
			subscribeInfo.setPtOrgId(subscribeTime.getPtOrgId());
			subscribeInfo.setOpOrgId(subscribeTime.getOpOrgId());
			subscribeInfo.setStatus(subscribeTime.getStatus());			
			subscribeInfo.setCreateDate(subscribeTime.getCreateDate());
			subscribeInfo.setCreateUserid(subscribeTime.getCreateUserid());
			subscribeInfo.setUpdateTime(subscribeTime.getUpdateTime());
			subscribeInfo.setOperatorId(subscribeTime.getOperatorId());
			subscribeInfo.setChecksum(subscribeTime.getChecksum());
			subscribeInfo.setDistributeStatus("0");
			subscribeInfoDAO.createSubscribeInfo(subscribeInfo);
			count++;
			collSi.add(subscribeInfo);
		}
		return count;
	}
	private boolean createSubscribeDateTime(Collection<SubscribeTime> collSt, SubscribeDate subscribeDate, ProducerWork producerWork){
		Date startTime = subscribeDate.getSubscribeStartTime();
		Calendar cal=Calendar.getInstance();
		cal.setTime(startTime);
		cal.add(Calendar.MINUTE, subscribeDate.getIntervalTime());
		Date dTime = cal.getTime();
		
		int subscribeMax = 0;		
		while(dTime.getTime() <= subscribeDate.getSubscribeEndTime().getTime()){
			SubscribeTime stPojo = new SubscribeTime();		
			
			stPojo.setSubscribeDateId(subscribeDate.getId());
			stPojo.setProducerType(subscribeDate.getProducerType());
			stPojo.setProducerId(subscribeDate.getProducerId());
			stPojo.setTeamId(subscribeDate.getTeamId());
			stPojo.setDepartmentId(subscribeDate.getDepartmentId());
			stPojo.setOrgId(subscribeDate.getOrgId());
			stPojo.setWorkDate(DateUtil2.parseDate(DateUtil2.formatDate(startTime, "yyyy-MM-dd"), "yyyy-MM-dd"));			
			stPojo.setStartTime(DateUtil2.formatDate(startTime, "HH:mm"));
			stPojo.setEndTime(DateUtil2.formatDate(dTime, "HH:mm"));
			stPojo.setSubscribeStartTime(startTime);
			stPojo.setSubscribeEndTime(dTime);
			stPojo.setFeeId(subscribeDate.getFeeId());
			stPojo.setSubscribeStatus(producerWork.getPublishStatus());	
			stPojo.setSubscribeMax(subscribeDate.getIntervalNum());		
			stPojo.setSubscribeNum(Integer.valueOf(0));
			stPojo.setPtOrgId(subscribeDate.getPtOrgId());
			stPojo.setOpOrgId(producerWork.getOpOrgId());
			stPojo.setStatus(subscribeDate.getStatus());
			stPojo.setCreateDate(subscribeDate.getCreateDate());
			stPojo.setCreateUserid(subscribeDate.getCreateUserid());
			stPojo.setUpdateTime(producerWork.getUpdateTime());
			stPojo.setOperatorId(producerWork.getOperatorId());
			stPojo.setChecksum(subscribeDate.getChecksum());			
			collSt.add(stPojo);
			
			subscribeMax += subscribeDate.getIntervalNum();
			startTime = dTime;
			cal.add(Calendar.MINUTE, subscribeDate.getIntervalTime());
			dTime  = cal.getTime();	
		}
		long msec = subscribeDate.getSubscribeEndTime().getTime() - startTime.getTime();
		if(msec > 0 && msec < (subscribeDate.getIntervalTime() * 60000)){
			 int residue = (int)(msec * subscribeDate.getIntervalNum() + subscribeDate.getIntervalTime() * 30000) / (subscribeDate.getIntervalTime() * 60000);
			 if(residue > 0 && residue <= subscribeDate.getIntervalNum()){
				 SubscribeTime stPojo = new SubscribeTime();
				 stPojo.setSubscribeDateId(subscribeDate.getId());
				 stPojo.setProducerType(subscribeDate.getProducerType());
				 stPojo.setProducerId(subscribeDate.getProducerId());
				 stPojo.setTeamId(subscribeDate.getTeamId());
				 stPojo.setDepartmentId(subscribeDate.getDepartmentId());
				 stPojo.setOrgId(subscribeDate.getOrgId());
				 stPojo.setWorkDate(DateUtil2.parseDate(DateUtil2.formatDate(startTime, "yyyy-MM-dd"), "yyyy-MM-dd"));	
				 stPojo.setStartTime(DateUtil2.formatDate(startTime, "HH:mm"));
				 stPojo.setEndTime(DateUtil2.formatDate(subscribeDate.getSubscribeEndTime(), "HH:mm"));
				 stPojo.setSubscribeStartTime(startTime);
				 stPojo.setSubscribeEndTime(subscribeDate.getSubscribeEndTime());
				 stPojo.setFeeId(subscribeDate.getFeeId());
				 stPojo.setSubscribeStatus(producerWork.getPublishStatus());	
				 stPojo.setSubscribeMax(residue);		
				 stPojo.setSubscribeNum(Integer.valueOf(0));
				 stPojo.setPtOrgId(subscribeDate.getPtOrgId());
				 stPojo.setOpOrgId(producerWork.getOpOrgId());
				 stPojo.setStatus(subscribeDate.getStatus());
				 stPojo.setCreateDate(subscribeDate.getCreateDate());
				 stPojo.setCreateUserid(subscribeDate.getCreateUserid());
				 stPojo.setUpdateTime(producerWork.getUpdateTime());
				 stPojo.setOperatorId(producerWork.getOperatorId());
				 stPojo.setChecksum(subscribeDate.getChecksum());			
				 collSt.add(stPojo);					
				 subscribeMax += residue;
			 }						
		}
		if(subscribeDate.getSubscribeMax().intValue() == subscribeMax){
			return true;
		}		
		return false;
	}
	private boolean createSubscribeDateTime(Collection<SubscribeTime> collSt, SubscribeDate subscribeDate){
		Date startTime = subscribeDate.getSubscribeStartTime();
		Calendar cal=Calendar.getInstance();
		cal.setTime(startTime);
		cal.add(Calendar.MINUTE, subscribeDate.getIntervalTime());
		Date dTime = cal.getTime();
		
		int subscribeMax = 0;		
		while(dTime.getTime() <= subscribeDate.getSubscribeEndTime().getTime()){
			SubscribeTime stPojo = new SubscribeTime();		
			
			stPojo.setSubscribeDateId(subscribeDate.getId());
			stPojo.setProducerType(subscribeDate.getProducerType());
			stPojo.setProducerId(subscribeDate.getProducerId());
			stPojo.setTeamId(subscribeDate.getTeamId());
			stPojo.setDepartmentId(subscribeDate.getDepartmentId());
			stPojo.setOrgId(subscribeDate.getOrgId());
			stPojo.setWorkDate(DateUtil2.parseDate(DateUtil2.formatDate(startTime, "yyyy-MM-dd"), "yyyy-MM-dd"));			
			stPojo.setStartTime(DateUtil2.formatDate(startTime, "HH:mm"));
			stPojo.setEndTime(DateUtil2.formatDate(dTime, "HH:mm"));
			stPojo.setSubscribeStartTime(startTime);
			stPojo.setSubscribeEndTime(dTime);
			stPojo.setFeeId(subscribeDate.getFeeId());
			stPojo.setSubscribeStatus(ScheduleConstants.PUBLISH_STATUS_MEDIUM);	
			stPojo.setSubscribeMax(subscribeDate.getIntervalNum());		
			stPojo.setSubscribeNum(Integer.valueOf(0));
			stPojo.setPtOrgId(subscribeDate.getPtOrgId());
			stPojo.setOpOrgId(subscribeDate.getOpOrgId());
			stPojo.setStatus(subscribeDate.getStatus());
			stPojo.setCreateDate(subscribeDate.getUpdateTime());
			stPojo.setCreateUserid(subscribeDate.getOperatorId());
			stPojo.setUpdateTime(subscribeDate.getUpdateTime());
			stPojo.setOperatorId(subscribeDate.getOperatorId());
			stPojo.setChecksum(subscribeDate.getChecksum());			
			collSt.add(stPojo);
			
			subscribeMax += subscribeDate.getIntervalNum();
			startTime = dTime;
			cal.add(Calendar.MINUTE, subscribeDate.getIntervalTime());
			dTime  = cal.getTime();	
		}
		long msec = subscribeDate.getSubscribeEndTime().getTime() - startTime.getTime();
		if(msec > 0 && msec < (subscribeDate.getIntervalTime() * 60000)){
			 int residue = (int)(msec * subscribeDate.getIntervalNum() + subscribeDate.getIntervalTime() * 30000) / (subscribeDate.getIntervalTime() * 60000);
			 if(residue > 0 && residue <= subscribeDate.getIntervalNum()){
				 SubscribeTime stPojo = new SubscribeTime();
				 stPojo.setSubscribeDateId(subscribeDate.getId());
				 stPojo.setProducerType(subscribeDate.getProducerType());
				 stPojo.setProducerId(subscribeDate.getProducerId());
				 stPojo.setTeamId(subscribeDate.getTeamId());
				 stPojo.setDepartmentId(subscribeDate.getDepartmentId());
				 stPojo.setOrgId(subscribeDate.getOrgId());
				 stPojo.setWorkDate(DateUtil2.parseDate(DateUtil2.formatDate(startTime, "yyyy-MM-dd"), "yyyy-MM-dd"));	
				 stPojo.setStartTime(DateUtil2.formatDate(startTime, "HH:mm"));
				 stPojo.setEndTime(DateUtil2.formatDate(subscribeDate.getSubscribeEndTime(), "HH:mm"));
				 stPojo.setSubscribeStartTime(startTime);
				 stPojo.setSubscribeEndTime(subscribeDate.getSubscribeEndTime());
				 stPojo.setFeeId(subscribeDate.getFeeId());
				 stPojo.setSubscribeStatus(ScheduleConstants.PUBLISH_STATUS_MEDIUM);	
				 stPojo.setSubscribeMax(residue);		
				 stPojo.setSubscribeNum(Integer.valueOf(0));
				 stPojo.setPtOrgId(subscribeDate.getPtOrgId());
				 stPojo.setOpOrgId(subscribeDate.getOpOrgId());
				 stPojo.setStatus(subscribeDate.getStatus());
				 stPojo.setCreateDate(subscribeDate.getCreateDate());
				 stPojo.setCreateUserid(subscribeDate.getCreateUserid());
				 stPojo.setUpdateTime(subscribeDate.getUpdateTime());
				 stPojo.setOperatorId(subscribeDate.getOperatorId());
				 stPojo.setChecksum(subscribeDate.getChecksum());			
				 collSt.add(stPojo);					
				 subscribeMax += residue;
			 }						
		}
		if(subscribeDate.getSubscribeMax().intValue() == subscribeMax){
			return true;
		}		
		return false;
	}
	private void decomposeWeekScheduleSubscribe(Collection<ProducerWork> input, ProducerWorkDTO pwDto){
		for (int week= 0; week < 7; week++) {
			String ruleIdJson = this.getRuleValue(pwDto, week);
			if( StringUtils.isNotBlank(ruleIdJson) ){
				this.decomposeDayScheduleSubscribe(input, pwDto, ruleIdJson, week);
			}
		}
	}
	private void decomposeDayScheduleSubscribe(Collection<ProducerWork> input, ProducerWorkDTO pwDto, String ruleIdJson, int week){		
	
		JSONObject jsonObject = JSONObject.fromObject(ruleIdJson);		
	    DayScheduleSubscribeDTO dayDto = (DayScheduleSubscribeDTO)JSONObject.toBean(jsonObject, DayScheduleSubscribeDTO.class);
	    if(dayDto.getNum() != null && dayDto.getNum().intValue() >= 0){
	    	ProducerWork pWork = new ProducerWork();
	    	
	    	pWork.setProducerId(pwDto.getProducerId());
	    	pWork.setTeamId(pwDto.getTeamId());
	    	pWork.setDepartmentId(pwDto.getDepartmentId());
	    	pWork.setOrgId(pwDto.getOrgId());
			
			Calendar cal= Calendar.getInstance();
			cal.setTime(DateUtil2.parseDate(pwDto.getStrMonday(), "yyyy-MM-dd"));
			cal.add(Calendar.DATE, week);
			pWork.setWorkDate(cal.getTime());			
			input.add(pWork);						
	    }
	}
	
	private void createMapProducerWorkTime(Map<Long, List<ProducerWork>> mapProducerWork, Collection<ProducerWork> collProducerWork){
		if(collProducerWork != null && collProducerWork.size() > 0){
			for (ProducerWork pWork : collProducerWork) {
				List<ProducerWork> innerList = mapProducerWork.get(pWork.getProducerId());
				if(innerList != null){
					innerList.add(pWork);
				}
				else{
					List<ProducerWork> newList = new ArrayList<ProducerWork>();
					newList.add(pWork);
					mapProducerWork.put(pWork.getProducerId(), newList);
				}
			}
		}		
	}
	private void batchInsertProducerWorkSubscribeDate(Collection<ProducerWork> okAdd, Map<String, List<ProducerWork>> mapInputProducerWork, Map<Long, List<ProducerWork>> mapNowProducerWork, Map<Long, List<SubscribeDate>> mapInputSd, Collection<ProducerWork> notInsert){
		int count = 0;
		if(okAdd != null && okAdd.size() > 0){
			for (ProducerWork okProducerWork : okAdd) {				
				String strKey = this.getMapKey(okProducerWork);
				List<ProducerWork> inputList = mapInputProducerWork.get(strKey);
				if(inputList == null){
					notInsert.add(okProducerWork);
					break;
				}
				Collection<ProducerWork> nowProducerWork = mapNowProducerWork.get(okProducerWork.getProducerId());
				if(nowProducerWork != null){
					if(this.judgeProducerWork(nowProducerWork, inputList)){
						nowProducerWork.addAll(inputList);
						count += this.InsertProducerWorkSubscribeDate(inputList, mapInputSd);
					}
					else{
						notInsert.add(okProducerWork);
					}
				}
				else{
					mapNowProducerWork.put(okProducerWork.getProducerId(), inputList);
					count += this.InsertProducerWorkSubscribeDate(inputList, mapInputSd);
				}				
				if( count >= 200 ){
					count = 0;
					dao.flush();
				}
			}
			dao.flush();
		}		
	}
	
	private void batchInsertProducerWorkSubscribeDateTime(Collection<ProducerWork> okAdd, Map<String, List<ProducerWork>> mapInputProducerWork, Map<Long, List<ProducerWork>> mapNowProducerWork, Map<Long, List<SubscribeDate>> mapInputSd, Collection<ProducerWork> notInsert){
		int count = 0;
		if(okAdd != null && okAdd.size() > 0){
			for (ProducerWork okProducerWork : okAdd) {				
				String strKey = this.getMapKey(okProducerWork);
				List<ProducerWork> inputList = mapInputProducerWork.get(strKey);
				if(inputList == null){
					notInsert.add(okProducerWork);
					break;
				}
				Collection<ProducerWork> nowProducerWork = mapNowProducerWork.get(okProducerWork.getProducerId());
				if(nowProducerWork != null){
					if(this.judgeProducerWork(nowProducerWork, inputList)){
						nowProducerWork.addAll(inputList);
						// TODO
						// TODO
						count += this.InsertProducerWorkSubscribeDateTime(inputList, mapInputSd);
					}
					else{
						notInsert.add(okProducerWork);
					}
				}
				else{
					mapNowProducerWork.put(okProducerWork.getProducerId(), inputList);
					count += this.InsertProducerWorkSubscribeDateTime(inputList, mapInputSd);
				}				
				if( count >= 200 ){
					count = 0;
					dao.flush();
				}
			}
			dao.flush();
		}		
	}
	
	private void batchInsertProducerWorkSubscribeDate(Collection<ProducerWork> okAdd, Map<String, List<ProducerWork>> mapInputProducerWork, Collection<ProducerWork> nowProducerWork, Map<Long, List<SubscribeDate>> mapInputSd, Collection<ProducerWork> notInsert){
		int count = 0;
		if(okAdd != null && okAdd.size() > 0){
			for (ProducerWork okProducerWork : okAdd) {
				String strKey = this.getMapKey(okProducerWork);
				Collection<ProducerWork> inputList = mapInputProducerWork.get(strKey);
				if(inputList == null){
					notInsert.add(okProducerWork);
					break;
				}
				if(this.judgeProducerWork(nowProducerWork, inputList)){
					nowProducerWork.addAll(inputList);
					count += this.InsertProducerWorkSubscribeDate(inputList, mapInputSd);
				}
				else{
					notInsert.add(okProducerWork);
				}
				if( count >= 200 ){
					count = 0;
					dao.flush();
				}
			}
			dao.flush();
		}		
	}
	private int InsertProducerWorkSubscribeDate(Collection<ProducerWork> collProducerWork, Map<Long, List<SubscribeDate>> mapInputSd){
		int count = 0;
		if(collProducerWork != null && collProducerWork.size() > 0){
			for (ProducerWork pWork : collProducerWork) {
				List<SubscribeDate> sdList = mapInputSd.get(pWork.getId());
				if(sdList != null && sdList.size() > 0){
					pWork.setSubscribeFlag(Integer.valueOf(1));
					for (SubscribeDate subscribeDate : sdList) {
						subscribeDateDAO.createSubscribeDate(subscribeDate);
						count++;
					}
				}
				producerWorkDAO.createProducerWork(pWork);
				count++;
			}
		}			
		return count;
	}
	private int InsertProducerWorkSubscribeDateTime(Collection<ProducerWork> collProducerWork, Map<Long, List<SubscribeDate>> mapInputSd){
		int count = 0;
		if(collProducerWork != null && collProducerWork.size() > 0){
			for (ProducerWork pWork : collProducerWork) {
				List<SubscribeDate> sdList = mapInputSd.get(pWork.getId());
				if(sdList != null && sdList.size() > 0){
					pWork.setSubscribeFlag(Integer.valueOf(1));
					for (SubscribeDate subscribeDate : sdList) {
						// TODO
						// TODO
						subscribeDateDAO.createSubscribeDate(subscribeDate);
						count++;
						count += this.createSubscribeTimeSubscribeInfo(subscribeDate);
					}
				}
				producerWorkDAO.createProducerWork(pWork);
				count++;
			}
		}			
		return count;
	}
	private void batchSelectProducerWorkTime(Collection<ProducerWork> nowProducerWork, ProducerWorkDTO producerWorkDto){	
		Calendar cal= Calendar.getInstance();
		cal.setTime(DateUtil2.parseDate(producerWorkDto.getStrMonday(), "yyyy-MM-dd"));
		cal.add(Calendar.DATE, 6 + 6);
		ProducerWork pWork = new ProducerWork();
		pWork.setWorkStartTime(DateUtil2.parseDate(producerWorkDto.getStrMonday(), "yyyy-MM-dd"));
		pWork.setWorkEndTime(cal.getTime());
		pWork.setProducerType(producerWorkDto.getProducerType());
		pWork.setProducerId(producerWorkDto.getProducerId());
		Collection<ProducerWork> collProducerWork = producerWorkDAO.getProducerWorkList(pWork);
		nowProducerWork.addAll(collProducerWork);
	}
	private void batchSelectProducerWorkTime(Map<Long, List<ProducerWorkDTO>> mapPwDto, Collection<ProducerWork> nowProducerWork, ProducerWorkDTO producerWorkDto){	
		if(producerWorkDto.getPublishStatus() == ScheduleConstants.PUBLISH_STATUS_AUDITED){
			return;
		}
		List<ProducerWorkDTO> innerList = mapPwDto.get(producerWorkDto.getProducerId());
		if(innerList != null){
			innerList.add(producerWorkDto);
		}
		else{
			List<ProducerWorkDTO> newList = new ArrayList<ProducerWorkDTO>();
			newList.add(producerWorkDto);
			mapPwDto.put(producerWorkDto.getProducerId(), newList);
			this.batchSelectProducerWorkTime(nowProducerWork, producerWorkDto);
		}
	}
	private void decomposeMayAddBySame(Collection<ProducerWork> mayAdd, Collection<ProducerWork> same, Collection<ProducerWork> okAdd){
		if(mayAdd != null && mayAdd.size() > 0){
			for (ProducerWork addProducerWork : mayAdd) {
				boolean flag = true;
				if(same != null && same.size() > 0){
					for (ProducerWork sameProducerWork : same) {
						if(this.judgeProducerWork(addProducerWork, sameProducerWork)){
							flag = false;
							break;
						}
					}
				}				
				if(flag){
					okAdd.add(addProducerWork);
				}
			}
		}		
	}
	private void decomposeMayDelbyMayAdd(Collection<ProducerWork> mayDel, Collection<ProducerWork> mayAdd, Collection<ProducerWork> okDel, Collection<ProducerWork> same){
		if(mayDel != null && mayDel.size() > 0){
			for (ProducerWork delProducerWork : mayDel) {
				boolean flag = true;
				if(mayAdd != null && mayAdd.size() > 0){
					for (ProducerWork addProducerWork : mayAdd) {
						if(this.judgeProducerWorkRuleId(delProducerWork, addProducerWork)){
							same.add(delProducerWork);
							flag = false;
							break;
						}
					}
				}				
				if(flag){
					okDel.add(delProducerWork);
				}
			}
		}		
	}
	private void batchDeleteProducerWorkSubscribeDate(Collection<ProducerWork> okDel, Map<String, List<ProducerWork>> mapOldProducerWork, ProducerWork producerWork, Map<Long, List<SubscribeDate>> mapOldSubscribeDate){
		int count = 0;
		if(okDel != null && okDel.size() > 0){
			for (ProducerWork okWork : okDel) {
				String strKey = this.getMapKey(okWork);
				Collection<ProducerWork> collProducerWork = mapOldProducerWork.get(strKey);
				if(collProducerWork != null && collProducerWork.size() > 0){
					for (ProducerWork pWork : collProducerWork) {
						List<SubscribeDate> sdList = mapOldSubscribeDate.get(pWork.getId());
						if(sdList != null && sdList.size() > 0){
							for (SubscribeDate subscribeDate : sdList) {
								//subscribeDate.setPublishStatus(Integer.valueOf(9));
								subscribeDate.setStatus("I");
								subscribeDate.setUpdateTime(producerWork.getUpdateTime());
								subscribeDate.setOperatorId(producerWork.getOperatorId());
								subscribeDate.setOpOrgId(producerWork.getOpOrgId());
								subscribeDateDAO.updateSubscribeDate(subscribeDate);
								count++;
							}
						}
						//pWork.setPublishStatus(Integer.valueOf(9));
						pWork.setStatus("I");
						pWork.setUpdateTime(producerWork.getUpdateTime());
						pWork.setOperatorId(producerWork.getOperatorId());
						pWork.setOpOrgId(producerWork.getOpOrgId());
						producerWorkDAO.updateProducerWork(pWork);
						count++;
						if( count >= 200 ){
							count = 0;
							dao.flush();
						}
					}
				}				
			}
			dao.flush();
		}		
	}
	private void batchUpdateSubscribeDate(Collection<ProducerWork> same, Map<String, List<ProducerWork>> mapOldProducerWork, Map<String, List<ProducerWork>> mapInputProducerWork, ProducerWork producerWork, Map<Long, List<SubscribeDate>> mapOldSd, Map<Long, List<SubscribeDate>> mapInputSd){
		int count = 0;
		if(same != null && same.size() > 0){
			for (ProducerWork sameProducerWork : same) {
				String strKey = this.getMapKey(sameProducerWork);

				List<ProducerWork> oldProducerWorkList = mapOldProducerWork.get(strKey);				
				if(oldProducerWorkList == null || oldProducerWorkList.size() < 1){
					return;
				}
				List<ProducerWork> inputProducerWorkList = mapInputProducerWork.get(strKey);
				if(inputProducerWorkList == null || inputProducerWorkList.size() < 1){
					return;
				}
				if(oldProducerWorkList.size() != inputProducerWorkList.size()){
					return;
				}
				for (ProducerWork oldProducerWork : oldProducerWorkList) {
					List<SubscribeDate> oldSdList = mapOldSd.get(oldProducerWork.getId());
					if(oldSdList != null && oldSdList.size() > 0){
						List<SubscribeDate> inputSdList = null;
						for (ProducerWork inputProducerWork : inputProducerWorkList) {
							if(this.judgeProducerWorkTime(oldProducerWork, inputProducerWork)){
								inputSdList = mapInputSd.get(inputProducerWork.getId());								
								break;
							}							
						}
						for (SubscribeDate oldSd : oldSdList){
							boolean flag = true;
							if(inputSdList != null && inputSdList.size() > 0){
								for (SubscribeDate inputSd : inputSdList) {
									if(this.judgeSubscribeDate(inputSd, oldSd)){
										flag = false;
										break;
									}
								}
							}
							if(flag){
								oldSd.setStatus("I");
								oldSd.setUpdateTime(producerWork.getUpdateTime());
								oldSd.setOperatorId(producerWork.getOperatorId());
								oldSd.setOpOrgId(producerWork.getOpOrgId());
								subscribeDateDAO.updateSubscribeDate(oldSd);
								count++;
							}
						}
						if(inputSdList == null || inputSdList.size() < 1){
							oldProducerWork.setSubscribeFlag(Integer.valueOf(0));
							oldProducerWork.setUpdateTime(producerWork.getUpdateTime());
							oldProducerWork.setOperatorId(producerWork.getOperatorId());
							oldProducerWork.setOpOrgId(producerWork.getOpOrgId());
							producerWorkDAO.updateProducerWork(oldProducerWork);
							count++;
						}
					}					
				}
				
				for (ProducerWork oldProducerWork : oldProducerWorkList) {
					List<SubscribeDate> oldSdList = mapOldSd.get(oldProducerWork.getId());
					List<SubscribeDate> inputSdList = null;
					for (ProducerWork inputProducerWork : inputProducerWorkList) {
						if(this.judgeProducerWorkTime(oldProducerWork, inputProducerWork)){
							inputSdList = mapInputSd.get(inputProducerWork.getId());								
							break;
						}							
					}
					if(inputSdList != null && inputSdList.size() > 0){
						for (SubscribeDate inputSd : inputSdList){
							boolean flag = true;
							if(oldSdList != null && oldSdList.size() > 0){
								for (SubscribeDate oldSd : oldSdList) {
									if(this.judgeSubscribeDate(inputSd, oldSd)){
										flag = false;
										break;
									}
								}
							}
							if(flag){
								inputSd.setProducerWorkId(oldProducerWork.getId());
								subscribeDateDAO.createSubscribeDate(inputSd);
								count++;
							}
						}
						if(oldSdList == null || oldSdList.size() == 0){
							oldProducerWork.setSubscribeFlag(Integer.valueOf(1));
							oldProducerWork.setUpdateTime(producerWork.getUpdateTime());
							oldProducerWork.setOperatorId(producerWork.getOperatorId());
							oldProducerWork.setOpOrgId(producerWork.getOpOrgId());
							producerWorkDAO.updateProducerWork(oldProducerWork);
							count++;
						}
					}				
				}
				if( count >= 200 ){
					count = 0;
					dao.flush();
				}
			}
			dao.flush();
		}		
	}
	private void batchUpdateSubscribeDateTime(Collection<ProducerWork> same, Map<String, List<ProducerWork>> mapOldProducerWork, Map<String, List<ProducerWork>> mapInputProducerWork, ProducerWork producerWork, Map<Long, List<SubscribeDate>> mapOldSd, Map<Long, List<SubscribeDate>> mapInputSd){
		int count = 0;
		if(same != null && same.size() > 0){
			for (ProducerWork sameProducerWork : same) {
				String strKey = this.getMapKey(sameProducerWork);

				List<ProducerWork> oldProducerWorkList = mapOldProducerWork.get(strKey);				
				if(oldProducerWorkList == null || oldProducerWorkList.size() < 1){
					return;
				}
				List<ProducerWork> inputProducerWorkList = mapInputProducerWork.get(strKey);
				if(inputProducerWorkList == null || inputProducerWorkList.size() < 1){
					return;
				}
				if(oldProducerWorkList.size() != inputProducerWorkList.size()){
					return;
				}
				for (ProducerWork oldProducerWork : oldProducerWorkList) {
					List<SubscribeDate> oldSdList = mapOldSd.get(oldProducerWork.getId());
					if(oldSdList != null && oldSdList.size() > 0){
						List<SubscribeDate> inputSdList = null;
						for (ProducerWork inputProducerWork : inputProducerWorkList) {
							if(this.judgeProducerWorkTime(oldProducerWork, inputProducerWork)){
								inputSdList = mapInputSd.get(inputProducerWork.getId());								
								break;
							}							
						}
						for (SubscribeDate oldSd : oldSdList){
							boolean flag = true;
							if(inputSdList != null && inputSdList.size() > 0){
								for (SubscribeDate inputSd : inputSdList) {
									if(this.judgeSubscribeDate(inputSd, oldSd)){
										flag = false;
										break;
									}
								}
							}
							if(flag){
								oldSd.setStatus("I");
								oldSd.setUpdateTime(producerWork.getUpdateTime());
								oldSd.setOperatorId(producerWork.getOperatorId());
								oldSd.setOpOrgId(producerWork.getOpOrgId());
								subscribeDateDAO.updateSubscribeDate(oldSd);
								count++;
							}
						}
					}					
				}
				
				for (ProducerWork oldProducerWork : oldProducerWorkList) {
					List<SubscribeDate> oldSdList = mapOldSd.get(oldProducerWork.getId());
					List<SubscribeDate> inputSdList = null;
					int subscribeFlag = 0;
					for (ProducerWork inputProducerWork : inputProducerWorkList) {
						if(this.judgeProducerWorkTime(oldProducerWork, inputProducerWork)){
							inputSdList = mapInputSd.get(inputProducerWork.getId());								
							break;
						}							
					}
					if(inputSdList != null && inputSdList.size() > 0){
						subscribeFlag = 1;
						for (SubscribeDate inputSd : inputSdList){
							boolean flag = true;
							if(oldSdList != null && oldSdList.size() > 0){
								for (SubscribeDate oldSd : oldSdList) {
									if(this.judgeSubscribeDate(inputSd, oldSd)){
										oldSd.setPublishStatus(producerWork.getPublishStatus());
										oldSd.setUpdateTime(producerWork.getUpdateTime());
										oldSd.setOperatorId(producerWork.getOperatorId());
										oldSd.setOpOrgId(producerWork.getOpOrgId());
										subscribeDateDAO.updateSubscribeDate(oldSd);
										count++;
										count += this.createSubscribeTimeSubscribeInfo(oldSd);
										flag = false;
										break;
									}
								}
							}
							if(flag){
								inputSd.setProducerWorkId(oldProducerWork.getId());
								// TODO
								// TODO
								subscribeDateDAO.createSubscribeDate(inputSd);								
								count++;
								count += this.createSubscribeTimeSubscribeInfo(inputSd);
							}
						}
					}
					oldProducerWork.setSubscribeFlag(subscribeFlag);
					oldProducerWork.setPublishStatus(producerWork.getPublishStatus());
					oldProducerWork.setUpdateTime(producerWork.getUpdateTime());
					oldProducerWork.setOperatorId(producerWork.getOperatorId());
					oldProducerWork.setOpOrgId(producerWork.getOpOrgId());
					producerWorkDAO.updateProducerWork(oldProducerWork);
					count++;
				}
				if( count >= 200 ){
					count = 0;
					dao.flush();
				}
			}
			dao.flush();
		}		
	}
	private int createSubscribeTimeSubscribeInfo(SubscribeDate subscribeDate){
		int count = 0;
		Collection<SubscribeTime> collSt = new ArrayList<SubscribeTime>();
		this.createSubscribeDateTime(collSt, subscribeDate);
		Collection<SubscribeInfo> collSi = new ArrayList<SubscribeInfo>();
		for (SubscribeTime subscribeTime : collSt) {
			subscribeTimeDAO.createSubscribeTime(subscribeTime);
			count++;
			//count += this.createSubscribeInfo(collSi, subscribeTime);
		}
		/*for (SubscribeInfo subscribeInfo : collSi) {
			subscribeInfoDAO.createSubscribeInfo(subscribeInfo);
			count++;
		}*/
		return count;
	}
	private void decomposeInputByNotDel(Collection<ProducerWork> input, Collection<ProducerWork> notDel, Collection<ProducerWork> mayAdd, Collection<ProducerWork> notAdd){
		if(input != null && input.size() > 0){
			for (ProducerWork inputProducerWork : input) {
				boolean flag = true;
				if(notDel != null && notDel.size() >0){
					for (ProducerWork notProducerWork : notDel) {
						if(this.judgeProducerWork(inputProducerWork, notProducerWork)){
							notAdd.add(inputProducerWork);
							flag = false;
							break;
						}
					}
				}				
				if(flag){
					mayAdd.add(inputProducerWork);
				}
			}
		}		
	}
	
	private void batchSelectWeekSchedule(Collection<ProducerWork> old, Map<String, List<ProducerWork>> mapOldProducerWork, ProducerWorkDTO producerWorkDto){
		if(producerWorkDto.getPublishStatus() == ScheduleConstants.PUBLISH_STATUS_AUDITED){
			return;
		}
		ProducerWork pwPojo = new ProducerWork();
		pwPojo.setProducerType(producerWorkDto.getProducerType());
		pwPojo.setProducerId(producerWorkDto.getProducerId());
		pwPojo.setTeamId(producerWorkDto.getTeamId());
		pwPojo.setDepartmentId(producerWorkDto.getDepartmentId());
		pwPojo.setOrgId(producerWorkDto.getOrgId());
		pwPojo.setWorkDate(DateUtil2.parseDate(producerWorkDto.getStrMonday(), "yyyy-MM-dd"));
		Collection<ProducerWork> collProducerWork = producerWorkDAO.getProducerWorkWeekListByToday(pwPojo);
		if(collProducerWork != null && collProducerWork.size() > 0){
			for (ProducerWork pWork : collProducerWork) {
				String strKey = this.getMapKey(pWork);
				List<ProducerWork> innerList = mapOldProducerWork.get(strKey);
				if(innerList != null){
					innerList.add(pWork);
				}
				else{
					List<ProducerWork> oldList = new ArrayList<ProducerWork>();
					oldList.add(pWork);
					mapOldProducerWork.put(strKey, oldList);
					old.add(pWork);
				}
			}
		}		
	}
	private void batchSelectWeekSubscribeDate(Collection<SubscribeDate> oldSubscribeDate, ProducerWorkDTO producerWorkDto){
		SubscribeDate sdPojo = new SubscribeDate();
		sdPojo.setProducerType(producerWorkDto.getProducerType());
		sdPojo.setProducerId(producerWorkDto.getProducerId());		
		sdPojo.setTeamId(producerWorkDto.getTeamId());
		sdPojo.setDepartmentId(producerWorkDto.getDepartmentId());
		sdPojo.setOrgId(producerWorkDto.getOrgId());
		sdPojo.setWorkDate(DateUtil2.parseDate(producerWorkDto.getStrMonday(), "yyyy-MM-dd"));
		Collection<SubscribeDate> collSubscribeDate = subscribeDateDAO.getSubscribeDateWeekList(sdPojo);
		oldSubscribeDate.addAll(collSubscribeDate);
	}
	private void decomposeOldProducerWork(Collection<ProducerWork> old, Collection<ProducerWork> mayDel, Collection<ProducerWork> notDel){
		if(old != null && old.size() > 0){
			for (ProducerWork pWork : old) {
				if(pWork.getPublishStatus().intValue() == ScheduleConstants.PUBLISH_STATUS_AUDITED){
					notDel.add(pWork);
				}							
				else{
					mayDel.add(pWork);
				}
			}
		}
	}
	private void getMapScheduleRule(Map<String, List<ScheduleRule>> mapScheduleRule, Long orgId){
		ScheduleRule srPojo = new ScheduleRule();
		srPojo.setOrgId(orgId);
		Collection<ScheduleRule> collRule = scheduleRuleDAO.getScheduleRuleList(srPojo);
		if(collRule != null && collRule.size() > 0){
			for (ScheduleRule scheduleRule : collRule) {
				List<ScheduleRule> innerList = mapScheduleRule.get(scheduleRule.getRuleId());
				if(innerList != null){
					innerList.add(scheduleRule);
				}
				else{
					List<ScheduleRule> inputList = new ArrayList<ScheduleRule>();
					inputList.add(scheduleRule);
					mapScheduleRule.put(scheduleRule.getRuleId(), inputList);
				}
			}
		}		
	}
	private String getRuleValue(ProducerWorkDTO producerWorkDto, int iWeek){
		switch(iWeek){
		case 0:
			return producerWorkDto.getRuleMonday();
		case 1:
			return producerWorkDto.getRuleTuesday();
		case 2:
			return producerWorkDto.getRuleWednesday();
		case 3:
			return producerWorkDto.getRuleThursday();
		case 4:
			return producerWorkDto.getRuleFriday();
		case 5:
			return producerWorkDto.getRuleSaturday();
		case 6:
			return producerWorkDto.getRuleSunday();		
		default:
			return null;		
		}
	}
	private void decomposeWeekSchedule(Collection<ProducerWork> input, Map<String, List<ProducerWork>> mapInputProducerWork, Map<Long, List<SubscribeDate>> mapInputSubscribeDate, ProducerWork producerWork, ProducerWorkDTO producerWorkDto, Map<String, List<ScheduleRule>> mapScheduleRule){
		if(producerWorkDto.getPublishStatus() == ScheduleConstants.PUBLISH_STATUS_AUDITED){
			return;
		}
		for (int week= 0; week < 7; week++) {
			String ruleIdJson = this.getRuleValue(producerWorkDto, week);
			if( StringUtils.isNotBlank(ruleIdJson) ){
				this.decomposeDaySchedule(input, mapInputProducerWork, mapInputSubscribeDate, producerWork, producerWorkDto, mapScheduleRule, ruleIdJson, week);
			}
		}		
	}
	private int compareDate(Date d1, Date d2){
		Date date1 = DateUtil2.parseDate(DateUtil2.formatDate(d1, "yyyy-MM-dd"), "yyyy-MM-dd");			
		Date date2 = DateUtil2.parseDate(DateUtil2.formatDate(d2, "yyyy-MM-dd"), "yyyy-MM-dd");
		return date1.compareTo(date2);
	}
	private void decomposeDaySchedule(Collection<ProducerWork> input, Map<String, List<ProducerWork>> mapInputProducerWork, Map<Long, List<SubscribeDate>> mapInputSd, ProducerWork producerWork, ProducerWorkDTO producerWorkDto, Map<String, List<ScheduleRule>> mapScheduleRule, String ruleIdJson, int week){	
		JSONObject jsonObject = JSONObject.fromObject(ruleIdJson);		
	    DayScheduleSubscribeDTO dayDto = (DayScheduleSubscribeDTO)JSONObject.toBean(jsonObject, DayScheduleSubscribeDTO.class);
	    List<ScheduleRule> listSr = mapScheduleRule.get(dayDto.getRuleId());
	    if(listSr != null && listSr.size() > 0){
	    	Calendar cal= Calendar.getInstance();
	    	Date todayDate = cal.getTime();
			cal.setTime(DateUtil2.parseDate(producerWorkDto.getStrMonday(), "yyyy-MM-dd"));
			cal.add(Calendar.DATE, week);
			Date workDate = cal.getTime();
			if(this.compareDate(todayDate, workDate) > 0){
				return;
			}
			String strKey = null;
	    	for (ScheduleRule sr : listSr) {
	    		ProducerWork pWork = new ProducerWork();
		    	pWork.setId((Long)CustomGenerator.generate());
				pWork.setProducerType(producerWorkDto.getProducerType());
				pWork.setProducerId(producerWorkDto.getProducerId());
				pWork.setTeamId(producerWorkDto.getTeamId());
				pWork.setDepartmentId(producerWorkDto.getDepartmentId());
				pWork.setOrgId(producerWorkDto.getOrgId());
				pWork.setWorkDate(workDate);				
				
				pWork.setRuleId(sr.getRuleId());
				pWork.setStartTime(sr.getStartTime());
				pWork.setEndTime(sr.getEndTime());
				pWork.setPublishStatus(producerWork.getPublishStatus());	
				pWork.setSubscribeFlag(Integer.valueOf(0));
				pWork.setCreateUserid(producerWork.getCreateUserid());
				pWork.setUpdateTime(producerWork.getUpdateTime());
				pWork.setOperatorId(producerWork.getOperatorId());
				pWork.setChecksum(producerWork.getChecksum());		
				pWork.setPtOrgId(producerWork.getPtOrgId());
				pWork.setOpOrgId(producerWork.getOpOrgId());
				pWork.setStatus(producerWork.getStatus());
				pWork.setCreateDate(producerWork.getCreateDate());
				
				pWork.setWorkMinutes(sr.getWorkMinutes());
				pWork.setSpanDays(sr.getSpanDays());
				pWork.setWorkStartTime(this.getWorkStartTime(workDate, sr.getSpanDays(), sr.getStartTime()));				
				pWork.setWorkEndTime(this.getWorkEndTime(pWork.getWorkStartTime(), pWork.getWorkMinutes()));				
				
				strKey = this.getMapKey(pWork);
				List<ProducerWork> innerList = mapInputProducerWork.get(strKey);
				if(innerList != null){
					innerList.add(pWork);
				}
				else{
					List<ProducerWork> producerWorkList = new ArrayList<ProducerWork>();
					producerWorkList.add(pWork);
					mapInputProducerWork.put(strKey, producerWorkList);
					input.add(pWork);
				}
			}
	    	List<SubscribeDate> subscribeDateList = new ArrayList<SubscribeDate>();
	    	List<ProducerWork> producerWorkList = mapInputProducerWork.get(strKey);
	    	if(producerWorkList != null && producerWorkList.size() > 0){
	    		this.createDaySubscribeDate(subscribeDateList, producerWorkList, dayDto);
	    	}						
			if(subscribeDateList != null && subscribeDateList.size() > 0){
				for (SubscribeDate subscribeDate : subscribeDateList) {
					List<SubscribeDate> innerList = mapInputSd.get(subscribeDate.getProducerWorkId());
					if(innerList != null){
						innerList.add(subscribeDate);
					}
					else{
						List<SubscribeDate> newList = new ArrayList<SubscribeDate>();
						newList.add(subscribeDate);
						mapInputSd.put(subscribeDate.getProducerWorkId(), newList);
					}
				}
			}
	    }
	}
	private int createDaySubscribeDate(List<SubscribeDate> subscribeDateList, List<ProducerWork> producerWorkList, DayScheduleSubscribeDTO dayDto){
		if(dayDto.getNum() != null && dayDto.getInterval() != null && dayDto.getSubNum() != null){
			int sum = dayDto.getNum().intValue();
			for(int num=0; num < sum; num++){
				String startTime = this.getSubscribeStartTimeValue(dayDto, num);
				String endTime = this.getSubscribeEndTimeValue(dayDto, num);
				Long feeId = this.getSubscribeFeeValue(dayDto, num);
				if(startTime != null && endTime != null && feeId != null){
					this.createSubscribeDate(subscribeDateList, producerWorkList, dayDto, startTime, endTime, feeId);
				}
			}
		}		
		return 0;
	}
	
	private int createSubscribeDate(List<SubscribeDate> subscribeDateList, List<ProducerWork> producerWorkList, DayScheduleSubscribeDTO dayDto, String startTime, String endTime, Long feeId){
		if(producerWorkList != null && producerWorkList.size() > 0){
			for (ProducerWork pWork : producerWorkList) {
				Date subscribeStartTime = DateUtil2.parseDate(DateUtil2.formatDate(pWork.getWorkStartTime(), "yyyy-MM-dd") + " " + startTime + ":00", "yyyy-MM-dd HH:mm:ss");
				if(subscribeStartTime.before(pWork.getWorkStartTime())){
					Calendar cal=Calendar.getInstance();
					cal.setTime(subscribeStartTime);
					cal.add(Calendar.DATE, 1);
					subscribeStartTime = cal.getTime();
				}		
				Date subscribeEndTime = DateUtil2.parseDate(DateUtil2.formatDate(pWork.getWorkEndTime(), "yyyy-MM-dd") + " " + endTime + ":00", "yyyy-MM-dd HH:mm:ss");
				if(subscribeEndTime.after(pWork.getWorkEndTime())){
					Calendar cal=Calendar.getInstance();
					cal.setTime(subscribeEndTime);
					cal.add(Calendar.DATE, -1);
					subscribeEndTime = cal.getTime();
				}
				
				if(subscribeStartTime.before(subscribeEndTime)){
					Calendar cal=Calendar.getInstance();
					cal.setTime(subscribeStartTime);
					cal.add(Calendar.MINUTE, dayDto.getInterval()); 
					Date dTime = cal.getTime();
					
					int subscribeMax = 0;
					while(dTime.getTime() <= subscribeEndTime.getTime()){
						subscribeMax += dayDto.getSubNum();
						cal.add(Calendar.MINUTE, dayDto.getInterval());
						dTime  = cal.getTime();
					}
					long msec = subscribeEndTime.getTime() + dayDto.getInterval() * 60000 - dTime.getTime();
					if(msec > 0 && msec < (dayDto.getInterval() * 60000)){
						 int residue = (int)(msec * dayDto.getSubNum() + dayDto.getInterval() * 30000) / (dayDto.getInterval() * 60000);
						 if(residue > 0 && residue <= dayDto.getSubNum()){
							 subscribeMax += residue;
						 }						
					}
					if(subscribeMax > 0){
						SubscribeDate sdPojo = new SubscribeDate();    		
			    		sdPojo.setProducerWorkId(pWork.getId());
			    		sdPojo.setProducerType(pWork.getProducerType());
			    		sdPojo.setProducerId(pWork.getProducerId());
			    		sdPojo.setTeamId(pWork.getTeamId());
						sdPojo.setDepartmentId(pWork.getDepartmentId());			
						sdPojo.setWorkDate(DateUtil2.parseDate(DateUtil2.formatDate(subscribeStartTime, "yyyy-MM-dd"), "yyyy-MM-dd"));
						sdPojo.setStartTime(startTime);
						sdPojo.setEndTime(endTime);
						sdPojo.setFeeId(feeId);
						sdPojo.setPublishStatus(pWork.getPublishStatus());
						sdPojo.setSubscribeMax(subscribeMax);	
						sdPojo.setSubscribeNum(Integer.valueOf(0));	
						sdPojo.setOrgId(pWork.getOrgId());
			    		sdPojo.setPtOrgId(pWork.getPtOrgId());
						sdPojo.setOpOrgId(pWork.getOpOrgId());
						sdPojo.setStatus(pWork.getStatus());
						sdPojo.setCreateDate(pWork.getCreateDate());
						sdPojo.setCreateUserid(pWork.getCreateUserid());
						sdPojo.setUpdateTime(pWork.getUpdateTime());
						sdPojo.setOperatorId(pWork.getOperatorId());
						sdPojo.setChecksum(pWork.getChecksum());
						sdPojo.setSubscribeStartTime(subscribeStartTime);			
						sdPojo.setSubscribeEndTime(subscribeEndTime);
						sdPojo.setIntervalTime(dayDto.getInterval());
						sdPojo.setIntervalNum(dayDto.getSubNum());
						if(this.judgeSubscribeDate(subscribeDateList, sdPojo)){
							subscribeDateList.add(sdPojo);
							pWork.setSubscribeFlag(Integer.valueOf(1));
							return 0;
						}
					}		
					break;
				}
			}
		}		
		
		return 1;
	}
	
	
	private void setProducerWorkDtoRuleValue(Collection<ProducerWorkDTO> coll,  ProducerWorkDTO pwDto){
		if(coll != null && coll.size() > 0){
			Date dMonday = DateUtil2.parseDate(pwDto.getStrMonday(), "yyyy-MM-dd");
			ProducerWork pwPojo = new ProducerWork();
			pwPojo.setProducerType(pwDto.getProducerType());
			pwPojo.setProducerId(pwDto.getProducerId());
			pwPojo.setTeamId(pwDto.getTeamId());
			pwPojo.setDepartmentId(pwDto.getDepartmentId());
			pwPojo.setOrgId(pwDto.getOrgId());
			//pwPojo.setPublishStatus(pwDto.getPublishStatus());
			if(pwDto.getCopyFlag() != null && pwDto.getCopyFlag() == ScheduleConstants.COPY_FLAG){
				Calendar cal= Calendar.getInstance();
				cal.setTime(dMonday);
				cal.add(Calendar.DATE, -7);
				dMonday = cal.getTime();
			}
			pwPojo.setWorkDate(dMonday);			

			Collection<ProducerWork> collProducerWork = producerWorkDAO.getProducerWorkWeekList(pwPojo);			
			Map<String, List<ProducerWork>> mapProducerWork = new HashMap<String, List<ProducerWork>>();

			if(collProducerWork != null && collProducerWork.size() > 0){
				for (ProducerWork producerWork: collProducerWork) {
					String strKey = this.getMapDtoKey(producerWork);
					List<ProducerWork> innerList = mapProducerWork.get(strKey);
					if(innerList != null){
						innerList.add(producerWork);
					}
					else{
						List<ProducerWork> oldList = new ArrayList<ProducerWork>();
						oldList.add(producerWork);
						mapProducerWork.put(strKey, oldList);
					}
				}
			}			
			
			SubscribeDate sdPojo = new SubscribeDate();
			sdPojo.setProducerType(pwDto.getProducerType());
			sdPojo.setProducerId(pwDto.getProducerId());
			sdPojo.setTeamId(pwDto.getTeamId());
			sdPojo.setDepartmentId(pwDto.getDepartmentId());
			sdPojo.setOrgId(pwDto.getOrgId());
			//sdPojo.setPublishStatus(pwDto.getPublishStatus());
			sdPojo.setWorkDate(dMonday);
			Collection<SubscribeDate> collSubscribeDate = subscribeDateDAO.getSubscribeDateWeekList(sdPojo);
			Map<Long, List<SubscribeDate>> mapSubscribeDate = new HashMap<Long, List<SubscribeDate>>();
			this.createMapSubscribeDate(mapSubscribeDate, collSubscribeDate);

			Iterator<ProducerWorkDTO> iterator = coll.iterator();
			while (iterator.hasNext()) {
				ProducerWorkDTO producerWorkDTO = iterator.next();
				producerWorkDTO.setStrMonday(pwDto.getStrMonday());
				if(pwDto.getCopyFlag() != null && pwDto.getCopyFlag() == ScheduleConstants.COPY_FLAG){
					producerWorkDTO.setPublishStatus(ScheduleConstants.PUBLISH_STATUS_EDIT);
				}else{
					producerWorkDTO.setPublishStatus(ScheduleConstants.PUBLISH_STATUS_AUDITING);
				}				
				for (int week = 0; week < 7; week++) {
					this.updateRuleValue(mapProducerWork, mapSubscribeDate, producerWorkDTO, dMonday, pwDto.getPublishStatus(), week);
				}
			}
		}
	}
	
	private void updateRuleValue(Map<String, List<ProducerWork>> mapProducerWork, Map<Long, List<SubscribeDate>> mapSubscribeDate, ProducerWorkDTO producerWorkDTO, Date dMonday, Integer publishStatus, int week){
		Calendar cal= Calendar.getInstance();
		cal.setTime(dMonday);
		cal.add(Calendar.DATE, week);
		Date workDate = cal.getTime();
		String strKey = this.getMapKey(producerWorkDTO, workDate);
		List<ProducerWork> producerWorkList = mapProducerWork.get(strKey);
		if(producerWorkList != null && producerWorkList.size() > 0){
			DayScheduleSubscribeDTO dayDto = new DayScheduleSubscribeDTO();
			int num = 0;
			boolean flag = true;
			for (ProducerWork producerWork : producerWorkList) {				
				if(flag){
					dayDto.setRuleId(producerWork.getRuleId());
					dayDto.setStatus(producerWork.getPublishStatus());
					if(producerWorkDTO.getPublishStatus().intValue() == ScheduleConstants.PUBLISH_STATUS_EDIT){
						dayDto.setStatus(ScheduleConstants.PUBLISH_STATUS_EDIT);
					}else if(producerWork.getPublishStatus().intValue() == ScheduleConstants.PUBLISH_STATUS_AUDITED){
						producerWorkDTO.setPublishStatus(ScheduleConstants.PUBLISH_STATUS_AUDITED);
					}
					flag = false;
				}
				if(publishStatus != null && !publishStatus.equals(producerWork.getPublishStatus())){
					return;
				}
				List<SubscribeDate> subscribeDateList = mapSubscribeDate.get(producerWork.getId());
				if(subscribeDateList != null && subscribeDateList.size() > 0){
					for (SubscribeDate subscribeDate : subscribeDateList) {
						this.setSubscribeStartTimeValue(dayDto, subscribeDate.getStartTime(), num);
						this.setSubscribeEndTimeValue(dayDto, subscribeDate.getEndTime(), num);
						this.setSubscribeFeeValue(dayDto, subscribeDate.getFeeId(), num);
						if(num == 0){
							dayDto.setInterval(subscribeDate.getIntervalTime());
							dayDto.setSubNum(subscribeDate.getIntervalNum());
						}
						num++;
					}
				}
			}
			if(num > 5){
				num = 5;
			}
			dayDto.setNum(Integer.valueOf(num));
			this.setRuleValue(producerWorkDTO, JSONObject.fromObject(dayDto, Config.jsonConfig2).toString(), week);
		}		
	}
	private void setRuleValue(ProducerWorkDTO producerWorkDTO, String ruleId, int iWeek){
		switch(iWeek){
		case 0:
			producerWorkDTO.setRuleMonday(ruleId);
			break;
		case 1:
			producerWorkDTO.setRuleTuesday(ruleId);
			break;
		case 2:
			producerWorkDTO.setRuleWednesday(ruleId);
			break;
		case 3:
			producerWorkDTO.setRuleThursday(ruleId);
			break;
		case 4:
			producerWorkDTO.setRuleFriday(ruleId);
			break;
		case 5:
			producerWorkDTO.setRuleSaturday(ruleId);
			break;
		case 6:
			producerWorkDTO.setRuleSunday(ruleId);
			break;		
		default:
			break;		
		}
	}
	private void createMapSubscribeDate(Map<Long, List<SubscribeDate>> mapSubscribeDate, Collection<SubscribeDate> collSubscribeDate){
		if(collSubscribeDate != null && collSubscribeDate.size() > 0){
			for (SubscribeDate sd : collSubscribeDate) {
				List<SubscribeDate> innerList = mapSubscribeDate.get(sd.getProducerWorkId());
				if(innerList != null && innerList.size() > 0){
					innerList.add(sd);
				}
				else{
					List<SubscribeDate> newList = new ArrayList<SubscribeDate>();
					newList.add(sd);
					mapSubscribeDate.put(sd.getProducerWorkId(), newList);
				}
			}
		}
	}
	
	private void setSubscribeFeeValue(DayScheduleSubscribeDTO dayDto, Long feeId, int num){
		switch(num){
		case 0:
			dayDto.setFee0(feeId);
			break;
		case 1:
			dayDto.setFee1(feeId);
			break;
		case 2:
			dayDto.setFee2(feeId);
			break;
		case 3:
			dayDto.setFee3(feeId);
			break;
		case 4:
			dayDto.setFee4(feeId);
			break;
		default:
			break;		
		}
	}
	
	private Long getSubscribeFeeValue(DayScheduleSubscribeDTO dayDto, int num){
		switch(num){
		case 0:
			return dayDto.getFee0();
		case 1:
			return dayDto.getFee1();
		case 2:
			return dayDto.getFee2();
		case 3:
			return dayDto.getFee3();
		case 4:
			return dayDto.getFee4();
		default:
			return null;		
		}
	}
	
	private void setSubscribeStartTimeValue(DayScheduleSubscribeDTO dayDto, String startTime, int num){
		switch(num){
		case 0:
			dayDto.setSt0(startTime);
			break;
		case 1:
			dayDto.setSt1(startTime);
			break;
		case 2:
			dayDto.setSt2(startTime);
			break;
		case 3:
			dayDto.setSt3(startTime);
			break;
		case 4:
			dayDto.setSt4(startTime);
			break;
		default:
			break;		
		}
	}
	
	private String getSubscribeStartTimeValue(DayScheduleSubscribeDTO dayDto, int num){
		switch(num){
		case 0:
			return dayDto.getSt0();
		case 1:
			return dayDto.getSt1();
		case 2:
			return dayDto.getSt2();
		case 3:
			return dayDto.getSt3();
		case 4:
			return dayDto.getSt4();
		default:
			return null;		
		}
	}
	
	private void setSubscribeEndTimeValue(DayScheduleSubscribeDTO dayDto, String endTime, int num){
		switch(num){
		case 0:
			dayDto.setEt0(endTime);
			break;
		case 1:
			dayDto.setEt1(endTime);
			break;
		case 2:
			dayDto.setEt2(endTime);
			break;
		case 3:
			dayDto.setEt3(endTime);
			break;
		case 4:
			dayDto.setEt4(endTime);
			break;
		default:
			break;
		}
	}
	
	private String getSubscribeEndTimeValue(DayScheduleSubscribeDTO dayDto, int num){
		switch(num){
		case 0:
			return dayDto.getEt0();
		case 1:
			return dayDto.getEt1();
		case 2:
			return dayDto.getEt2();
		case 3:
			return dayDto.getEt3();
		case 4:
			return dayDto.getEt4();
		default:
			return null;		
		}
	}
	private Date getWorkStartTime(Date workDate, Integer spanDays, String startTime){
		String strWorkDate = DateUtil2.formatDate(workDate, "yyyy-MM-dd");
		Date dWorkTime = DateUtil2.parseDate(strWorkDate + " " + startTime +":00", "yyyy-MM-dd HH:mm:ss");
		Calendar cal= Calendar.getInstance();
		cal.setTime(dWorkTime);
		cal.add(Calendar.DATE, spanDays.intValue());
		return cal.getTime();		
	}
	
	private Date getWorkStartTime(Date workDate, String startTime){
		String strWorkDate = DateUtil2.formatDate(workDate, "yyyy-MM-dd");
		Date dWorkTime = DateUtil2.parseDate(strWorkDate + " " + startTime +":00", "yyyy-MM-dd HH:mm:ss");
		return dWorkTime;
	}
	
	private Date getWorkEndTime(Date workStartTime, Integer workMinutes){
		Calendar cal= Calendar.getInstance();
		cal.setTime(workStartTime);
		cal.add(Calendar.MINUTE, workMinutes.intValue());
		return cal.getTime();
	}
	private boolean judgeSubscribeDate(List<SubscribeDate> sdList, SubscribeDate sdPojo){
		if(sdList != null && sdList.size() > 0){
			List<SubscribeDate> list = new ArrayList<SubscribeDate>();
			list.addAll(sdList);
			list.add(sdPojo);
			Collections.sort(list);
			int index = list.lastIndexOf(sdPojo);
			if((index+1) < list.size()){
				SubscribeDate end =list.get(index+1);
				if(sdPojo.getSubscribeEndTime().after(end.getSubscribeStartTime())){
					return false;
				}
			}
			if(index > 0){
				SubscribeDate start =list.get(index-1);
				if (sdPojo.getSubscribeStartTime().before(start.getSubscribeEndTime())) {
					return false;					
				}
			}
		}
		return true;
	}
	private boolean judgeProducerWork(ProducerWork judge, ProducerWork producerWork){
		if(judge.getProducerId().equals(producerWork.getProducerId()) 
				&& (judge.getWorkDate().getTime() == producerWork.getWorkDate().getTime())
				&& judge.getTeamId().equals(producerWork.getTeamId())	
				&& judge.getDepartmentId().equals(producerWork.getDepartmentId())				
				&& judge.getOrgId().equals(producerWork.getOrgId()) ){
			return true;
		}
		return false;
	}
	private boolean judgeProducerWorkRuleId(ProducerWork judge, ProducerWork producerWork){
		if(judgeProducerWork(judge, producerWork)){
			if(judge.getRuleId().equals(producerWork.getRuleId())){
				return true;
			}
		}
		return false;
	}
	private boolean judgeProducerWorkTime(ProducerWork judge, ProducerWork producerWork){
		if(this.judgeProducerWork(judge, producerWork)){
			if(judge.getWorkStartTime().getTime() == producerWork.getWorkStartTime().getTime()
					&& judge.getWorkEndTime().getTime() == producerWork.getWorkEndTime().getTime())
			return true;
		}
		return false;
	}
	private boolean judgeProducerWork(Collection<ProducerWork> list, ProducerWork producerWork){
		List<ProducerWork> listProducerWork = new ArrayList<ProducerWork>();
		listProducerWork.addAll(list);
		listProducerWork.add(producerWork);
		Collections.sort(listProducerWork);
		int index = listProducerWork.lastIndexOf(producerWork);
		if((index+1) < listProducerWork.size()){
			ProducerWork end =listProducerWork.get(index+1);
			if(producerWork.getWorkEndTime().after(end.getWorkStartTime())){
				return false;
			}
		}
		if(index > 0){
			ProducerWork start =listProducerWork.get(index-1);
			if (producerWork.getWorkStartTime().before(start.getWorkEndTime())) {
				return false;					
			}
		}		
		return true;
	}
	private boolean judgeProducerWork(Collection<ProducerWork> oldList, Collection<ProducerWork> newList){
		Collection<ProducerWork> listProducerWork = new ArrayList<ProducerWork>();
		listProducerWork.addAll(oldList);
		if(newList != null && newList.size() > 0){
			for (ProducerWork pWork : newList) {
				if(judgeProducerWork(listProducerWork, pWork)){
					listProducerWork.add(pWork);
				}
				else{
					return false;
				}
			}
		}
		return true;
	}
	private boolean judgeSubscribeDate(SubscribeDate judge, SubscribeDate subscribeDate){
		if( judge.getProducerId().equals(subscribeDate.getProducerId()) 
				&& judge.getTeamId().equals(subscribeDate.getTeamId())
				&& judge.getDepartmentId().equals(subscribeDate.getDepartmentId())
				&& judge.getOrgId().equals(subscribeDate.getOrgId())
				&& (judge.getSubscribeStartTime().getTime() == subscribeDate.getSubscribeStartTime().getTime())
				&& (judge.getSubscribeEndTime().getTime() == subscribeDate.getSubscribeEndTime().getTime())
				&& judge.getIntervalTime().equals(subscribeDate.getIntervalTime())				
				&& judge.getIntervalNum().equals(subscribeDate.getIntervalNum())
				&& judge.getFeeId().equals(subscribeDate.getFeeId()) ){ 
			
			return true;
		}
		return false;
	}	
	private String getMapKey(ProducerWork producerWork){
		String strKey = producerWork.getProducerId().toString() + producerWork.getTeamId().toString() + producerWork.getDepartmentId().toString() 
				+ producerWork.getOrgId().toString() + Long.toString(producerWork.getWorkDate().getTime()) + producerWork.getRuleId();
		return strKey;
	}
	private String getMapDtoKey(ProducerWork producerWork){
		String strKey = producerWork.getProducerId().toString() + producerWork.getTeamId().toString() + producerWork.getDepartmentId().toString() 
				+ producerWork.getOrgId().toString() + Long.toString(producerWork.getWorkDate().getTime());
		return strKey;
	}	
	private String getMapKey(ProducerWorkDTO producerWorkDTO, Date workDate){
		String strKey = producerWorkDTO.getProducerId().toString() + producerWorkDTO.getTeamId().toString() + producerWorkDTO.getDepartmentId().toString() 
				+ producerWorkDTO.getOrgId().toString()	+ Long.toString(workDate.getTime()); 
		return strKey;
	}
	
	public List<MontnDataDTO> getMonthData(String firstDay,
			String lastDay,ProducerWorkDTO pwDto) {
		List<MontnDataDTO> list =producerWorkDAO.getMonthData(firstDay, lastDay,pwDto);
		if(list!=null && list.size() > 0){
			for (MontnDataDTO montnDataDTO : list) {
				pwDto.setProducerId(Long.valueOf(montnDataDTO.getProducerId()));
			   List<WorkDate> info=producerWorkDAO.findByConditionWorkDate(firstDay,lastDay,pwDto);
			   montnDataDTO.setWorkDateList(info);
			}
		}
		return list;
	}

	public List<WorkDateInfo> getWorkDateInfo(String firstDay, String lastDay,
			ProducerWorkDTO pwDto,Long copyProducerId) {
		pwDto.setProducerId(copyProducerId);
		List<WorkDateInfo> list =producerWorkDAO.getWorkDateInfo(firstDay, lastDay,pwDto);
		if(list!=null && list.size() > 0){
			for (WorkDateInfo workDateInfo : list) {
				 List<WorkData> info=producerWorkDAO.findWorkDataInfo(firstDay,lastDay,pwDto,workDateInfo.getRuleId());
				 workDateInfo.setWorkTime(info);
			}
		}
		return list;
	}

}
