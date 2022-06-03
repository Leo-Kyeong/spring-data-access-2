package hello.itemservice.repository.jdbctemplate;

import hello.itemservice.domain.Item;
import hello.itemservice.repository.ItemRepository;
import hello.itemservice.repository.ItemSearchCond;
import hello.itemservice.repository.ItemUpdateDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SimpleJdbcInsert
 * INSERT SQL 을 편리하게 사용할 수 있다.
 */
@Slf4j
public class JdbcTemplateItemRepositoryV3 implements ItemRepository {

    private final NamedParameterJdbcTemplate template;
    // SimpleJdbcInsert 는 생성 시점에 데이터베이스 테이블의 메타 데이터를 조회
    // INSERT SQL 를 직접 작성하지 않아도 쉽게 사용할 수 있도록 기능 제공
    private final SimpleJdbcInsert jdbcInsert;

    /**
     * JdbcTemplate 은 데이터소스(dataSource)가 필요하다.
     * dataSource 를 의존 관계 주입 받고 생성자 내부에서 JdbcTemplate 을 생성
     * 스프링에서는 JdbcTemplate 을 사용할 때 관례상 이 방법을 많이 사용
     */
    public JdbcTemplateItemRepositoryV3(DataSource dataSource) {
        this.template = new NamedParameterJdbcTemplate(dataSource);
        this.jdbcInsert = new SimpleJdbcInsert(dataSource)
                .withTableName("item") // 데이터를 저장할 테이블 명 지정
                .usingGeneratedKeyColumns("id") // key 를 생성하는 PK 컬럼 명 지정
                .usingColumns("item_name", "price", "quantity"); // 생략 가능
    }

    @Override
    public Item save(Item item) {
        SqlParameterSource param = new BeanPropertySqlParameterSource(item);
        // executeAndReturnKey 을 사용해서 INSERT SQL 을 실행
        // 생성된 키 값도 매우 편리하게 조회
        Number key = jdbcInsert.executeAndReturnKey(param);
        item.setId(key.longValue());
        return item;
    }

    @Override
    public void update(Long itemId, ItemUpdateDto updateParam) {
        String sql = "update item " +
                "set item_name=:itemName, price=:price, quantity=:quantity where id=:id";

        // MapSqlParameterSource 는 Map 과 유사, SQL 에 좀 더 특화된 기능을 제공
        // ItemUpdateDto 에는 itemId 가 없기 때문에 BeanPropertySqlParameterSource 사용 불가
        // 대신에 MapSqlParameterSource 사용
        SqlParameterSource param = new MapSqlParameterSource()
                .addValue("itemName", updateParam.getItemName())
                .addValue("price", updateParam.getPrice())
                .addValue("quantity", updateParam.getQuantity())
                .addValue("id", itemId); // 이 부분이 별도로 필요하다.

        template.update(sql, param);
    }

    @Override
    public Optional<Item> findById(Long id) {
        String sql = "select id, item_name, price, quantity from item where id=:id";
        try {
            // queryForObject(sql, Map<String, ?>, RowMapper): 결과 로우가 하나일 때 사용
            // RowMapper 는 데이터베이스의 반환 결과인 ResultSet 을 객체로 변환
            Map<String, Object> param = Map.of("id", id); // Map 사용을 통한 바인딩
            Item item = template.queryForObject(sql, param, itemRowMapper());
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

        // 자바빈 프로퍼티 규약을 통해서 자동으로 파라미터 객체를 생성
        // getItemName() -> key=itemName, value=상품명
        SqlParameterSource param = new BeanPropertySqlParameterSource(cond);

        String sql = "select id, item_name, price, quantity from item";

        //동적 쿼리
        if (StringUtils.hasText(itemName) || maxPrice != null) {
            sql += " where";
        }

        boolean andFlag = false;
        if (StringUtils.hasText(itemName)) {
            sql += " item_name like concat('%',:itemName,'%')";
            andFlag = true;
        }

        if (maxPrice != null) {
            if (andFlag) {
                sql += " and";
            }
            sql += " price <= :maxPrice";
        }

        log.info("sql={}", sql);
        // query(sql, Map<String, ?>, RowMapper): 결과가 하나 이상일 때 사용
        // RowMapper 는 데이터베이스의 반환 결과인 ResultSet 을 객체로 변환
        // 결과가 없으면 빈 컬렉션을 반환
        return template.query(sql, param, itemRowMapper());
    }

    /**
     * 데이터베이스의 조회 결과를 객체로 변환할 때 사용
     * JDBC 의 ResultSet 과 동일
     */
    private RowMapper<Item> itemRowMapper() {

        // BeanPropertyRowMapper 는 ResultSet 의 결과를 받아서 자바빈 규약에 맞추어 데이터를 변환
        // 예) 데이터베이스에서 조회한 결과 이름(id) 을 기반으로 setId() 메서드를 호출하여 자동 변환
        // 만약 데이터베이스 컬럼 이름과 객체의 이름이 완전 다를 때 별칭(as)을 사용해서 문제를 해결
        // BeanPropertyRowMapper 는 언더스코어 표기법을 카멜로 자동 변환해준다. (camel 변환 지원)
        // DB 언더스코어 표기법(item_name) -> JAVA 카멜 표기법(itemName)
        return BeanPropertyRowMapper.newInstance(Item.class);
    }
}