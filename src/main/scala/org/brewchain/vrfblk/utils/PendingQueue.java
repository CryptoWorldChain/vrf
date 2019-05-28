package org.brewchain.vrfblk.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.brewchain.core.model.Block.BlockInfo;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class PendingQueue {
	// protected Cache<Long, TxArrays> storage;
	LoadingCache<Long, TxArrays> storage;
	public final static long STR_COUNTER = -1;
	private AtomicLong sizeCounter = new AtomicLong(0);

	public PendingQueue(String nameid, int maxElementsInMemory) {
		String cacheName = "pendingqueue_" + nameid;

		// PersistentCacheManager persistentCacheManager =
		// CacheManagerBuilder.newCacheManagerBuilder()
		// .with(CacheManagerBuilder.persistence("./db/ehcache"))
		// .withCache(cacheName,
		// CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class,
		// TxArrays.class,
		// ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10,
		// EntryUnit.ENTRIES) // 堆
		// .offheap(10, MemoryUnit.MB) // 堆外
		// .disk(1, MemoryUnit.GB) // 磁盘
		// )).build(true);
		//
		// storage = persistentCacheManager.getCache(cacheName, Long.class,
		// TxArrays.class);
		storage = CacheBuilder.newBuilder().refreshAfterWrite(5, TimeUnit.MINUTES).expireAfterWrite(5, TimeUnit.MINUTES)
				.expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(100).build(new CacheLoader<Long, TxArrays>() {
					@Override
					public TxArrays load(Long key) throws Exception {
						return null;
					}
				});
	}

	public void addElement(TxArrays hp) {
		long key = sizeCounter.incrementAndGet();
		storage.put(key, hp);
	}

	public void addLast(TxArrays hp) {
		addElement(hp);
	}

	public int size() {
		return (int) sizeCounter.get();
	}

	public TxArrays pollFirst() {
		List<TxArrays> ret = poll(1);

		if (ret != null && ret.size() > 0) {
			return ret.get(0);
		}

		return null;
	}

	public synchronized List<TxArrays> poll(int size) {
		List<TxArrays> ret = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			if (sizeCounter.get() == 0) {
				break;
			}
			long key = sizeCounter.getAndDecrement();
			TxArrays element = storage.getIfPresent(key);
			storage.invalidate(key);
			if (element != null) {
				ret.add(element);
			} else {
				if (sizeCounter.get() <= 0) {
					break;
				}
			}
		}
		return ret;
	}
}