/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package course;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

import java.util.List;

public class BlogPostDAO {
    DBCollection postsCollection;

    public BlogPostDAO(final DB blogDatabase) {
        postsCollection = blogDatabase.getCollection("posts");
    }

    public DBObject findByPermalink(String permalink) {
        DBObject post = postsCollection.findOne(new BasicDBObject("permalink", permalink));


        return post;
    }

    public List<DBObject> findByDateDescending(int limit) {
        List<DBObject> posts;
        DBCursor cursor = postsCollection.find().sort(new BasicDBObject().append("date", -1)).limit(limit);
        try {
            posts = cursor.toArray();
        } finally {
            cursor.close();
        }
        return posts;
    }

    public List<DBObject> findByTagDateDescending(final String tag) {
        List<DBObject> posts;
        BasicDBObject query = new BasicDBObject("tags", tag);
        System.out.println("/tag query: " + query.toString());
        DBCursor cursor = postsCollection.find(query).sort(new BasicDBObject().append("date", -1)).limit(10);
        try {
            posts = cursor.toArray();
        } finally {
            cursor.close();
        }
        return posts;
    }

    public String addPost(String title, String body, List tags, String username) {

        System.out.println("inserting blog entry " + title + " " + body);

        String permalink = title.replaceAll("\\s", "_"); // whitespace becomes _
        permalink = permalink.replaceAll("\\W", ""); // get rid of non alphanumeric
        permalink = permalink.toLowerCase();

        BasicDBObject post = new BasicDBObject("title", title);
        post.append("author", username);
        post.append("body", body);
        post.append("permalink", permalink);
        post.append("tags", tags);
        post.append("comments", new java.util.ArrayList());
        post.append("date", new java.util.Date());

        try {
            postsCollection.insert(post);
            System.out.println("Inserting blog post with permalink " + permalink);
        } catch (Exception e) {
            System.out.println("Error inserting post");
            return null;
        }

        return permalink;
    }

    public void addPostComment(final String name, final String email, final String body, final String permalink) {
        BasicDBObject comment = new BasicDBObject("author", name).append("body", body);
        if (email != null && !email.equals("")) {
            comment.append("email", email);
        }

        WriteResult result = postsCollection.update(new BasicDBObject("permalink", permalink),
                new BasicDBObject("$push", new BasicDBObject("comments", comment)), false, false);
    }

}
