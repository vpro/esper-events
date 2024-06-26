/*
 * Copyright (C) 2012 All rights reserved
 * VPRO The Netherlands
 */
package nl.vpro.esper.listener;

import lombok.Getter;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.internal.event.map.MapEventBean;
import com.espertech.esper.runtime.client.*;

/**
 * Creates a toplist with ratings from events aggregated with count(*) over a given time window, grouped
 * by a given key:
 * <p>
 * <code>select key, count(*) from TestEvent.win:time(1 min) group by key</code>
 * <p>
 * See test case for a working sample.
 * <p>
 * The list returned has a configurable size. When no new events arrive the list will drain unless the
 * keepSize config is set to true. When set to true, events falling outside the queried window are kept in the
 * list until they are updated or overtaken by newer events. On startup the list is empty.
 *
 * @since 1.1
 */
public class TopList implements UpdateListener {
    private final ConcurrentSkipListSet<Rating> topRatings = new ConcurrentSkipListSet<>();

    private final String key;

    private final int size;

    private final boolean keepSize;

    public TopList(String key, int size, boolean keepSize) {
        if(key == null) {
            throw new IllegalArgumentException("Key should not be null");
        }

        this.key = key;
        this.size = size;
        this.keepSize = keepSize;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        EventBean eventBean = newEvents[0];

        if(eventBean instanceof MapEventBean) {
            Map<String, Object> map = ((MapEventBean)newEvents[0]).getProperties();
            String id = (String)map.get(key);
            Long count = (Long)map.get("count(*)");

            Rating rating = new Rating(id, count);

            // ratings.remove(rating) does not remove on equals
            topRatings.removeIf(next -> next.equals(rating));

            // decrease events are fired as well do not add these when 0 is reached, i.e. when they fall
            // outside the given time range, unless explicitly configured to keep them
            if(count > 0 || keepSize) {
                topRatings.add(rating);
                cropSize();
            }
        }

    }

    public List<Rating> getTopRatings() {
        List<Rating> result = new ArrayList<>(topRatings);
        int ratingsSize = topRatings.size();
        int size = ratingsSize >= this.size ? this.size : ratingsSize;
        return result.subList(0, size);
    }

    private void cropSize() {
        while(topRatings.size() > 2 * size) {
            topRatings.remove(topRatings.last());
        }
    }

    @Getter
    public static final class Rating implements Comparable<Rating> {
        private final String key;

        private final Long score;

        private Rating(String key, Long score) {
            this.key = key;
            this.score = score;
        }

        @Override
        public String toString() {
            return "Rating" +
                "{key='" + key + '\'' +
                ", score=" + score +
                '}';
        }

        @Override
        public int compareTo(Rating rating) {
            if(!this.score.equals(rating.score)) {
                return rating.score.compareTo(this.score);
            }
            return rating.key.compareTo(this.key);
        }

        @Override
        public boolean equals(Object o) {
            if(this == o) {
                return true;
            }
            if(o == null || getClass() != o.getClass()) {
                return false;
            }

            Rating rating = (Rating)o;

            return !(key != null ? !key.equals(rating.key) : rating.key != null);

        }

        @Override
        public int hashCode() {
            return key != null ? key.hashCode() : 0;
        }
    }
}
