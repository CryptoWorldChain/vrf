package org.brewchain.vrfblk.utils;

import java.util.concurrent.atomic.AtomicLong;

import org.brewchain.mcore.tools.queue.FilePendingQueue;
import org.brewchain.mcore.tools.queue.IStorable;
import org.fc.zippo.dispatcher.IActorDispatcher;

import com.google.common.cache.LoadingCache;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class PendingQueue extends FilePendingQueue<TxArrays> {
	// protected Cache<Long, TxArrays> storage;
	LoadingCache<Long, TxArrays> storage;
	public final static long STR_COUNTER = -1;
	private AtomicLong sizeCounter = new AtomicLong(0);
	IActorDispatcher iddc;

	public PendingQueue(String nameid, IActorDispatcher iddc) {
		super(nameid);
		this.iddc = iddc;
		this.init(iddc);
	}

	@Override
	public void shutdown() {

	}

	@Override
	public IStorable newStoreObject() {
		return new TxArrays();
	}
}
