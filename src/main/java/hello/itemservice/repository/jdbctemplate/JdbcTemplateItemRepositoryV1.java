package hello.itemservice.repository.jdbctemplate;

import hello.itemservice.domain.Item;
import hello.itemservice.repository.ItemRepository;
import hello.itemservice.repository.ItemSearchCond;
import hello.itemservice.repository.ItemUpdateDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * JdbcTemplate
 * 순서 기반 파라미터 바인딩을 지원
 */
@Slf4j
public class JdbcTemplateItemRepositoryV1 implements ItemRepository {

    private final JdbcTemplate template;

    /**
     * JdbcTemplate 은 데이터소스(dataSource)가 필요하다.
     * dataSource 를 의존 관계 주입 받고 생성자 내부에서 JdbcTemplate 을 생성
     * 스프링에서는 JdbcTemplate 을 사용할 때 관례상 이 방법을 많이 사용
     */
    public JdbcTemplateItemRepositoryV1(DataSource dataSource) {
        this.template = new JdbcTemplate(dataSource);
    }

    @Override
    public Item save(Item item) {
        String sql = "insert into item(item_name, price, quantity) values(?,?,?)";
        KeyHolder keyHolder = new GeneratedKeyHolder(); // DB 에서 생성한 ID 값을 반환
        // 데이터를 변경할 때는 update() 를 사용
        // 반환 값은 int 인데, 영향 받은 로우 수를 반환
        template.update(connection -> {
            // 자동 증가 키
            PreparedStatement ps = connection.prepareStatement(sql, new String[]{"id"});
            ps.setString(1, item.getItemName());
            ps.setInt(2, item.getPrice());
            ps.setInt(3, item.getQuantity());
            return ps;
        }, keyHolder);

        long key = keyHolder.getKey().longValue();
        // 데이터베이스가 대신 생성해주는 PK ID 값은 데이터베이스가 생성하기 때문에
        // INSERT 가 완료 되어야 생성된 PK ID 값을 확인할 수 있다.
        // 그러므로 keyHolder 를 통해 key 값을 넘겨주어서 INSERT 이후에 ID 값을 설정한다.
        item.setId(key);
        return item;
    }

    @Override
    public void update(Long itemId, ItemUpdateDto updateParam) {
        String sql = "update item set item_name=?, price=?, quantity=? where id=?";
        // ? 에 바인딩할 파라미터를 순서대로 전달
        // 반환 값은 해당 쿼리의 영향을 받은 로우 수
        template.update(sql,
                updateParam.getItemName(),
                updateParam.getPrice(),
                updateParam.getQuantity(),
                itemId);
    }

    @Override
    public Optional<Item> findById(Long id) {
        String sql = "select id, item_name, price, quantity from item where id=?";
        try {
            // queryForObject(sql, RowMapper, Object... args): 결과 로우가 하나일 때 사용
            // RowMapper 는 데이터베이스의 반환 결과인 ResultSet 을 객체로 변환
            Item item = template.queryForObject(sql, itemRowMapper(), id);
            return Optional.of(item);
        // 결과가 없으면 EmptyResultDataAccessException 예외가 발생
        }catch (EmptyResultDataAccessException e){
            // 결과가 없을 때 Optional 을 반환해야 한다.
            // 따라서 결과가 없으면 예외를 잡아서 Optional.empty 를 대신 반환
            return Optional.empty();
        }
    }

    @Override
    public List<Item> findAll(ItemSearchCond cond) {
        String itemName = cond.getItemName();
        Integer maxPrice = cond.getMaxPrice();

        String sql = "select id, item_name, price, quantity from item";

        //동적 쿼리
        if (StringUtils.hasText(itemName) || maxPrice != null) {
            sql += " where";
        }

        boolean andFlag = false;
        List<Object> param = new ArrayList<>();
        if (StringUtils.hasText(itemName)) {
            sql += " item_name like concat('%',?,'%')";
            param.add(itemName);
            andFlag = true;
        }

        if (maxPrice != null) {
            if (andFlag) {
                sql += " and";
            }
            sql += " price <= ?";
            param.add(maxPrice);
        }

        log.info("sql={}", sql);
        // query(sql, RowMapper, Object... args): 결과가 하나 이상일 때 사용
        // RowMapper 는 데이터베이스의 반환 결과인 ResultSet 을 객체로 변환
        // 결과가 없으면 빈 컬렉션을 반환
        return template.query(sql, itemRowMapper(), param.toArray());
    }

    /**
     * 데이터베이스의 조회 결과를 객체로 변환할 때 사용
     * JDBC 의 ResultSet 과 동일
     */
    private RowMapper<Item> itemRowMapper() {
        return ((rs, rowNum) -> {
            Item item = new Item();
            item.setId(rs.getLong("id"));
            item.setItemName(rs.getString("item_name"));
            item.setPrice(rs.getInt("price"));
            item.setQuantity(rs.getInt("quantity"));
            return item;
        });
    }
}